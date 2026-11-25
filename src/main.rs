//! Servidor MCP do CAR por HTTP (streamable HTTP), em axum + rmcp.
//!
//! Expõe as consultas do carbr como ferramentas para assistentes. Não guarda dados: lê a
//! publicação (GeoParquet) direto do endereço público, com o DuckDB embutido, só os trechos
//! de que cada pergunta precisa.
//!
//!   CAR_URL   endereço da publicação (padrão https://car.castlab.org/current)
//!   BIND      endereço de escuta (padrão 127.0.0.1:8765)
//!   HOSTS     nomes aceitos no cabeçalho Host, separados por vírgula
//!             (padrão mcp.castlab.org,localhost,127.0.0.1)

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context};
use duckdb::Connection;
use rmcp::{
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ContentBlock, ServerCapabilities, ServerConfig},
    schemars, tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
    },
    ErrorData as McpError, ServerHandler,
};
use serde_json::{json, Map, Value};

const MAX_ROWS: usize = 200;
const CATALOG_TTL: Duration = Duration::from_secs(600);

const INSTRUCTIONS: &str = "\
Dados públicos do Cadastro Ambiental Rural (CAR) do Brasil, republicados do Sicar (Serviço \
Florestal Brasileiro): o perímetro declarado e dez atributos de cada um dos cerca de 8,6 milhões \
de imóveis rurais. Não há dado pessoal.

Ao responder com estes dados:
- O cadastro é autodeclarado. Há sobreposições, e constar no CAR não comprova posse nem \
propriedade (Lei 12.651/2012, art. 29, § 2º).
- Os dados são uma cópia semanal; informe a data da publicação (campo `versao`).
- Cite a fonte: Sicar/SFB, via carbr. É uma republicação independente, sem caráter oficial; em \
caso de divergência, vale o Sicar.
- Etapas da análise (`etapa`): waiting = aguardando análise; analysing = em análise; notified = \
analisado, com notificação sem resposta do declarante; answered = respondeu, aguarda nova análise; \
done = análise concluída; cancelled = cancelado.
- Porte (`size_class`): small = até 4 módulos fiscais; medium = de 4 a 15; large = acima de 15. \
Assentamentos e territórios tradicionais aparecem como small mesmo quando grandes.
- Para totais do país ou de um estado, use `resumo` ou `etapas_da_analise`; para um município, \
`municipio`.
- Qualidade: `overlap_share` é a parcela da área do imóvel sobreposta a outros imóveis do estado; \
`outside_share`, a parcela fora do município declarado. Sobreposição não é sinônimo de disputa: \
inclui imóvel declarado duas vezes e lote dentro de assentamento.
- Áreas protegidas: `indigenous_share` é a parcela do imóvel dentro de terras indígenas (Funai); \
`strict_protection_share` e `sustainable_use_share`, dentro de unidades de conservação de proteção \
integral e de uso sustentável (CNUC/MMA). Uso sustentável não é irregularidade: áreas de proteção \
ambiental (APA) e reservas particulares (RPPN) admitem imóveis privados.
- Tipologia (`tipologia`, em `municipio`): o grupo do município entre cinco, pelo perfil do cadastro \
(porte dos imóveis, etapa da análise e sobreposição); `typology_label` é o nome do grupo e os \
indicadores são parcelas de 0 a 1 (`coverage` é a área declarada sobre a área do município).";

/// Etapa da análise a partir do texto do Sicar (mesma regra dos pacotes e da página).
const STAGE: &str = "CASE WHEN condition LIKE 'Cancelado%' THEN 'cancelled' \
    WHEN condition = 'Aguardando análise' THEN 'waiting' \
    WHEN condition = 'Em análise' THEN 'analysing' \
    WHEN condition LIKE '%aguardando atendimento a notificação%' THEN 'notified' \
    WHEN condition LIKE '%após revisão%' THEN 'answered' \
    WHEN condition IS NULL THEN NULL ELSE 'done' END";

const LABEL: &str = "CASE etapa WHEN 'waiting' THEN 'Aguardando análise' \
    WHEN 'analysing' THEN 'Em análise' WHEN 'notified' THEN 'Notificado, sem resposta' \
    WHEN 'answered' THEN 'Respondeu, aguarda nova análise' WHEN 'done' THEN 'Analisado' \
    WHEN 'cancelled' THEN 'Cancelado' END";

const DIMS: [&str; 6] = ["state", "status", "size_class", "property_type", "condition", "created_year"];
const TOWN_ORDER: [&str; 6] =
    ["properties", "small_properties", "area_ha", "pending", "cancelled", "invalid_geometries"];
const PROPERTY_FIELDS: &str = "property_code, municipality, municipality_code, state, status, condition, property_type, \
    size_class, round(area_ha, 2) AS area_ha, round(fiscal_modules, 2) AS fiscal_modules, \
    strftime(created_at, '%Y-%m-%d') AS created_at";

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct Resumo {
    /// Dimensões do resumo, entre "state", "status", "size_class", "property_type", "condition"
    /// (texto do Sicar para a etapa da análise) e "created_year". Vazio dá o total.
    por: Option<Vec<String>>,
    /// Siglas das UFs (ex.: ["PE", "BA"]). Vazio é o país inteiro.
    uf: Option<Vec<String>>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct Ufs {
    /// Siglas das UFs (ex.: ["PA"]). Vazio é o país inteiro.
    uf: Option<Vec<String>>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct Municipios {
    /// Sigla da UF (ex.: "PE")
    uf: String,
    /// Coluna de ordenação: "properties" (imóveis, padrão), "small_properties", "area_ha",
    /// "pending", "cancelled" ou "invalid_geometries"
    ordenar_por: Option<String>,
    /// Quantos municípios devolver (padrão 20, máximo 200)
    limite: Option<usize>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct Municipio {
    /// Sigla da UF (ex.: "PA")
    uf: String,
    /// Nome do município, sem diferenciar maiúsculas nem acentos (ex.: "maraba")
    nome: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct Ponto {
    /// Longitude em graus decimais (negativa no Brasil; ex.: -47.9)
    lon: f64,
    /// Latitude em graus decimais (ex.: -15.8)
    lat: f64,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
struct Codigos {
    /// Códigos de imóvel no CAR, no formato UF-0000000-identificador (até 200)
    codigos: Vec<String>,
}

/// Acesso à publicação: uma base DuckDB em memória e o catálogo em cache.
#[derive(Clone)]
struct Data {
    base: Arc<String>,
    db: Arc<Mutex<Connection>>,
    catalog: Arc<Mutex<Option<(Instant, Arc<Value>)>>>,
}

impl Data {
    fn open(base: String) -> anyhow::Result<Self> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial; SET enable_progress_bar = false;")
            .context("não foi possível carregar as extensões httpfs e spatial do DuckDB")?;
        Ok(Self {
            base: Arc::new(base.trim_end_matches('/').to_string()),
            db: Arc::new(Mutex::new(db)),
            catalog: Arc::new(Mutex::new(None)),
        })
    }

    fn conn(&self) -> anyhow::Result<Connection> {
        Ok(self.db.lock().map_err(|_| anyhow!("conexão indisponível"))?.try_clone()?)
    }

    /// Linhas de uma consulta como JSON (o próprio DuckDB serializa).
    fn rows(&self, sql: &str) -> anyhow::Result<Vec<Value>> {
        let wrapped = format!("SELECT coalesce(to_json(list(t)), '[]')::VARCHAR FROM ({sql}) t");
        let text: String = self.conn()?.query_row(&wrapped, [], |row| row.get(0))?;
        Ok(serde_json::from_str(&text)?)
    }

    fn catalog(&self) -> anyhow::Result<Arc<Value>> {
        if let Some((at, cat)) = self.catalog.lock().unwrap().as_ref() {
            if at.elapsed() < CATALOG_TTL {
                return Ok(cat.clone());
            }
        }
        let sql = format!("SELECT content FROM read_text('{}/catalog.json')", self.base);
        let text: String = self.conn()?.query_row(&sql, [], |row| row.get(0))?;
        let cat = Arc::new(serde_json::from_str::<Value>(&text)?);
        *self.catalog.lock().unwrap() = Some((Instant::now(), cat.clone()));
        Ok(cat)
    }

    /// UFs pedidas, conferidas contra o catálogo; vazio = todas.
    fn states(&self, wanted: Option<&[String]>) -> anyhow::Result<Vec<String>> {
        let cat = self.catalog()?;
        let available: Vec<String> =
            cat["states"].as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
        match wanted {
            None | Some([]) => Ok(available),
            Some(list) => list
                .iter()
                .map(|uf| {
                    let uf = uf.trim().to_uppercase();
                    if available.contains(&uf) {
                        Ok(uf)
                    } else {
                        bail!("UF \"{uf}\" não está na publicação. Disponíveis: {}", available.join(", "))
                    }
                })
                .collect(),
        }
    }

    fn files(&self, kind: &str, states: &[String]) -> String {
        let list: Vec<String> = states
            .iter()
            .map(|uf| match kind {
                "properties" => format!("'{}/properties/state={uf}/part-0.parquet'", self.base),
                other => format!("'{}/{other}/state={uf}.parquet'", self.base),
            })
            .collect();
        format!("read_parquet([{}], union_by_name = true)", list.join(", "))
    }

    /// Toda resposta leva a data da publicação e a citação.
    fn envelope(&self, mut fields: Map<String, Value>) -> anyhow::Result<Value> {
        let cat = self.catalog()?;
        let version = cat["version"].as_str().unwrap_or("").to_string();
        fields.insert(
            "fonte".into(),
            json!(format!(
                "Serviço Florestal Brasileiro. Sistema Nacional de Cadastro Ambiental Rural (Sicar): \
                 imóveis rurais. Dados organizados pelo CastLab/UFPE (carbr), versão {version}."
            )),
        );
        fields.insert("versao".into(), json!(version));
        Ok(Value::Object(fields))
    }
}

fn fields(pairs: Vec<(&str, Value)>) -> Map<String, Value> {
    pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

/// `UF-0000000-` seguido de 32 dígitos hexadecimais.
fn valid_code(code: &str) -> bool {
    let b = code.as_bytes();
    b.len() == 43
        && b[..2].iter().all(u8::is_ascii_uppercase)
        && b[2] == b'-'
        && b[3..10].iter().all(u8::is_ascii_digit)
        && b[10] == b'-'
        && b[11..].iter().all(|c| c.is_ascii_digit() || (b'A'..=b'F').contains(c))
}

fn sql_text(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// Indicadores que `with_quality` acrescenta a cada imóvel.
const QUALITY_KEYS: [&str; 9] = [
    "overlap_count",
    "overlap_share",
    "outside_share",
    "indigenous_share",
    "strict_protection_share",
    "sustainable_use_share",
    "indigenous_land",
    "conservation_unit",
    "conservation_category",
];

/// "toda a área" ou "NN% da área".
fn percent(share: f64) -> String {
    if share >= 0.995 {
        "toda a área".into()
    } else {
        format!("{}% da área", ((100.0 * share).round() as i64).max(1))
    }
}

/// Acrescenta à triagem a sobreposição com terra indígena e unidade de conservação (mesma regra
/// de `car_check()` nos pacotes): acima de 1% da área, terra indígena e proteção integral levam
/// um "ok" a "attention"; uso sustentável só entra na nota.
fn protected(row: &Value, check: &'static str, note: &str) -> (&'static str, String, Value) {
    let share = |key: &str| row[key].as_f64().unwrap_or(0.0);
    let name = |key: &str| row[key].as_str().unwrap_or("?").to_string();
    let (ti, strict, sustainable) =
        (share("indigenous_share"), share("strict_protection_share"), share("sustainable_use_share"));
    let mut extra: Vec<String> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    if ti >= 0.01 {
        extra.push(format!("Sobrepõe terra indígena ({}) em {}.", name("indigenous_land"), percent(ti)));
        names.push(name("indigenous_land"));
    }
    if strict >= 0.01 {
        extra.push(format!(
            "Sobrepõe unidade de conservação de proteção integral ({}) em {}.",
            name("conservation_unit"),
            percent(strict)
        ));
        names.push(name("conservation_unit"));
    } else if sustainable >= 0.01 {
        names.push(name("conservation_unit"));
        if check != "blocked" {
            extra.push(format!(
                "Fica em unidade de conservação de uso sustentável ({}: {}), em {}.",
                name("conservation_category"),
                name("conservation_unit"),
                percent(sustainable)
            ));
        }
    }
    let alert = (ti >= 0.01 || strict >= 0.01) && check == "ok";
    let mut parts: Vec<String> = if alert { Vec::new() } else { vec![note.to_string()] };
    parts.extend(extra);
    let area = if names.is_empty() { Value::Null } else { json!(names.join("; ")) };
    (if alert { "attention" } else { check }, parts.join(" "), area)
}

/// Resultado da triagem de um imóvel (mesma regra de `car_check()` nos pacotes).
fn verdict(row: &Value) -> (&'static str, &'static str) {
    let status = row["status"].as_str();
    let stage = row["etapa"].as_str();
    if status == Some("cancelled") || stage == Some("cancelled") {
        ("blocked", "Cadastro cancelado.")
    } else if status == Some("suspended") {
        ("blocked", "Cadastro suspenso.")
    } else if stage == Some("notified") {
        ("attention", "Analisado, com notificação sem resposta do declarante.")
    } else if status == Some("pending") {
        ("attention", "Cadastro pendente.")
    } else if status.is_none() {
        ("attention", "Situação não reconhecida.")
    } else {
        ("ok", "Cadastro ativo, sem notificação pendente.")
    }
}

// ---- Consultas (bloqueantes; rodam em spawn_blocking) ----

impl Data {
    fn catalogo(&self) -> anyhow::Result<Value> {
        let cat = self.catalog()?;
        let states: Vec<Value> = cat["states"]
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(uf, s)| {
                        json!({"state": uf, "properties": s["properties"], "municipalities": s["municipalities"],
                               "downloaded_to": s["downloaded_to"]})
                    })
                    .collect()
            })
            .unwrap_or_default();
        let total: i64 = states.iter().filter_map(|s| s["properties"].as_i64()).sum();
        self.envelope(fields(vec![("estados", json!(states)), ("total_de_imoveis", json!(total))]))
    }

    fn dicionario(&self) -> anyhow::Result<Value> {
        let rows = self.rows(&format!("SELECT * FROM read_json('{}/dictionary.json')", self.base))?;
        self.envelope(fields(vec![("colunas", json!(rows))]))
    }

    fn resumo(&self, p: Resumo) -> anyhow::Result<Value> {
        let dims = p.por.unwrap_or_default();
        if let Some(bad) = dims.iter().find(|d| !DIMS.contains(&d.as_str())) {
            bail!("Dimensão \"{bad}\" desconhecida. Use: {}", DIMS.join(", "));
        }
        let states = self.states(p.uf.as_deref())?;
        let mut select = dims.clone();
        if dims.iter().any(|d| d == "condition") {
            select.push(format!("{STAGE} AS etapa"));
        }
        select.push("sum(properties)::BIGINT AS properties".into());
        select.push("round(sum(area_ha), 2) AS area_ha".into());
        let group = if dims.is_empty() { String::new() } else { " GROUP BY ALL ORDER BY ALL".into() };
        let sql = format!(
            "SELECT {} FROM {}{group} LIMIT {}",
            select.join(", "),
            self.files("summary", &states),
            MAX_ROWS + 1
        );
        let mut rows = self.rows(&sql)?;
        let truncated = rows.len() > MAX_ROWS;
        rows.truncate(MAX_ROWS);
        self.envelope(fields(vec![("linhas", json!(rows)), ("truncado", json!(truncated))]))
    }

    fn etapas(&self, p: Ufs) -> anyhow::Result<Value> {
        let states = self.states(p.uf.as_deref())?;
        let sql = format!(
            "WITH s AS (SELECT {STAGE} AS etapa, sum(properties)::BIGINT AS properties, round(sum(area_ha), 2) AS area_ha \
                        FROM {} GROUP BY ALL) \
             SELECT etapa, {LABEL} AS rotulo, properties, area_ha, \
                    round(100.0 * properties / sum(properties) OVER (), 1) AS percentual \
             FROM s ORDER BY properties DESC",
            self.files("summary", &states)
        );
        let rows = self.rows(&sql)?;
        let total: i64 = rows.iter().filter_map(|r| r["properties"].as_i64()).sum();
        self.envelope(fields(vec![("etapas", json!(rows)), ("total", json!(total))]))
    }

    fn municipios(&self, p: Municipios) -> anyhow::Result<Value> {
        let states = self.states(Some(&[p.uf]))?;
        let order = p.ordenar_por.unwrap_or_else(|| "properties".into());
        if !TOWN_ORDER.contains(&order.as_str()) {
            bail!("`ordenar_por` deve ser um de: {}", TOWN_ORDER.join(", "));
        }
        let limit = p.limite.unwrap_or(20).clamp(1, MAX_ROWS);
        let files = self.files("municipalities", &states);
        let rows = self.rows(&format!(
            "SELECT * EXCLUDE (xmin, ymin, xmax, ymax) REPLACE (round(area_ha, 2) AS area_ha) \
             FROM {files} ORDER BY {order} DESC LIMIT {limit}"
        ))?;
        let total = self.rows(&format!("SELECT count(*) AS n FROM {files}"))?;
        self.envelope(fields(vec![("municipios", json!(rows)), ("total_de_municipios", total[0]["n"].clone())]))
    }

    fn municipio(&self, p: Municipio) -> anyhow::Result<Value> {
        let states = self.states(Some(&[p.uf]))?;
        let files = self.files("municipalities", &states);
        let name = sql_text(p.nome.trim());
        let found = self.rows(&format!(
            "SELECT * EXCLUDE (xmin, ymin, xmax, ymax) REPLACE (round(area_ha, 2) AS area_ha) FROM {files} \
             WHERE strip_accents(lower(municipality)) = strip_accents(lower({name}))"
        ))?;
        let Some(town) = found.into_iter().next() else {
            let near = self.rows(&format!(
                "SELECT municipality FROM {files} \
                 WHERE strip_accents(lower(municipality)) LIKE '%' || strip_accents(lower({name})) || '%' \
                 ORDER BY municipality LIMIT 8"
            ))?;
            let names: Vec<Value> = near.into_iter().map(|r| r["municipality"].clone()).collect();
            return self.envelope(fields(vec![("encontrado", json!(false)), ("parecidos", json!(names))]));
        };
        let code = town["municipality_code"].as_i64().ok_or_else(|| anyhow!("município sem código"))?;
        let stages = self.rows(&format!(
            "WITH s AS (SELECT {STAGE} AS etapa, count(*) AS properties, round(sum(area_ha), 2) AS area_ha \
                        FROM {} WHERE municipality_code = {code} GROUP BY ALL) \
             SELECT etapa, {LABEL} AS rotulo, properties, area_ha, \
                    round(100.0 * properties / sum(properties) OVER (), 1) AS percentual \
             FROM s ORDER BY properties DESC",
            self.files("properties", &states)
        ))?;
        // Indicadores de qualidade do município; a falta deles não impede a resposta.
        let quality = self
            .rows(&format!(
                "SELECT * EXCLUDE (state, municipality_code), \
                        round(100.0 * overlapping / properties, 1) AS percentual_com_sobreposicao, \
                        round(100.0 * overlapping_half / properties, 1) AS percentual_com_mais_da_metade_sobreposta, \
                        round(100.0 * outside_half / properties, 1) AS percentual_com_mais_da_metade_fora_do_municipio \
                 FROM {} WHERE municipality_code = {code}",
                self.files("quality-municipalities", &states)
            ))
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .unwrap_or(Value::Null);
        // Grupo do município na tipologia; a falta dela não impede a resposta.
        let typology = self
            .rows(&format!(
                "SELECT * EXCLUDE (state, municipality_code, municipality) \
                 FROM read_parquet('{}/typology/municipalities.parquet') WHERE municipality_code = {code}",
                self.base
            ))
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .unwrap_or(Value::Null);
        self.envelope(fields(vec![
            ("encontrado", json!(true)),
            ("municipio", town),
            ("etapas", json!(stages)),
            ("qualidade", quality),
            ("tipologia", typology),
        ]))
    }

    /// Acrescenta os indicadores de qualidade (`QUALITY_KEYS`) a cada imóvel, quando a publicação
    /// os traz.
    fn with_quality(&self, mut rows: Vec<Value>) -> Vec<Value> {
        let mut by_state: BTreeMap<String, (Vec<i64>, Vec<String>)> = BTreeMap::new();
        for row in &rows {
            if let (Some(uf), Some(town), Some(code)) =
                (row["state"].as_str(), row["municipality_code"].as_i64(), row["property_code"].as_str())
            {
                let entry = by_state.entry(uf.to_string()).or_default();
                entry.0.push(town);
                entry.1.push(sql_text(code));
            }
        }
        let mut quality: HashMap<String, Value> = HashMap::new();
        for (uf, (towns, codes)) in by_state {
            let towns: Vec<String> = towns.iter().map(i64::to_string).collect();
            let found = self.rows(&format!(
                "SELECT * EXCLUDE (municipality_code) FROM {} \
                 WHERE municipality_code IN ({}) AND property_code IN ({})",
                self.files("quality", std::slice::from_ref(&uf)),
                towns.join(", "),
                codes.join(", ")
            ));
            for q in found.unwrap_or_default() {
                if let Some(code) = q["property_code"].as_str() {
                    quality.insert(code.to_string(), q);
                }
            }
        }
        for row in &mut rows {
            let q = row["property_code"].as_str().and_then(|code| quality.get(code));
            for key in QUALITY_KEYS {
                row[key] = q.map(|q| q[key].clone()).unwrap_or(Value::Null);
            }
        }
        rows
    }

    fn ponto(&self, p: Ponto) -> anyhow::Result<Value> {
        if !(-75.0..=-28.0).contains(&p.lon) || !(-35.0..=6.0).contains(&p.lat) {
            bail!("O ponto está fora do Brasil. Informe longitude e latitude em graus decimais (ex.: lon -47.9, lat -15.8).");
        }
        let (x, y) = (p.lon, p.lat);
        let all = self.states(None)?;
        let candidates = self.rows(&format!(
            "SELECT DISTINCT state FROM {} WHERE xmin <= {x} AND xmax >= {x} AND ymin <= {y} AND ymax >= {y}",
            self.files("municipalities", &all)
        ))?;
        let states: Vec<String> =
            candidates.iter().filter_map(|r| r["state"].as_str().map(String::from)).collect();
        let rows = if states.is_empty() {
            Vec::new()
        } else {
            self.rows(&format!(
                "SELECT {PROPERTY_FIELDS}, {STAGE} AS etapa FROM {} \
                 WHERE xmin <= {x} AND xmax >= {x} AND ymin <= {y} AND ymax >= {y} \
                   AND ST_Contains(geometry, ST_Point({x}, {y})) ORDER BY area_ha LIMIT {MAX_ROWS}",
                self.files("properties", &states)
            ))?
        };
        let rows = self.with_quality(rows);
        let n = rows.len();
        self.envelope(fields(vec![
            ("imoveis", json!(rows)),
            ("quantidade", json!(n)),
            ("sobreposicao", json!(n > 1)),
        ]))
    }

    fn verificar(&self, p: Codigos) -> anyhow::Result<Value> {
        if p.codigos.len() > MAX_ROWS {
            bail!("No máximo {MAX_ROWS} códigos por chamada; divida a lista.");
        }
        let mut codes: Vec<String> = Vec::new();
        for code in p.codigos {
            let code = code.trim().to_uppercase();
            if !code.is_empty() && !codes.contains(&code) {
                codes.push(code);
            }
        }
        let available = self.states(None)?;
        let mut by_state: BTreeMap<String, Vec<&String>> = BTreeMap::new();
        for code in codes.iter().filter(|c| valid_code(c) && available.contains(&c[..2].to_string())) {
            by_state.entry(code[..2].to_string()).or_default().push(code);
        }
        let list = |codes: &[&String]| codes.iter().map(|c| sql_text(c)).collect::<Vec<_>>().join(", ");
        let mut found: HashMap<String, Value> = HashMap::new();
        for (uf, wanted) in &by_state {
            let files = self.files("properties", std::slice::from_ref(uf));
            let mut towns: Vec<&str> = wanted.iter().map(|c| &c[3..10]).collect();
            towns.sort_unstable();
            towns.dedup();
            // O código traz o município: esse filtro lê poucos trechos do arquivo.
            let mut rows = self.rows(&format!(
                "SELECT {PROPERTY_FIELDS}, {STAGE} AS etapa FROM {files} \
                 WHERE municipality_code IN ({}) AND property_code IN ({})",
                towns.join(", "),
                list(wanted)
            ))?;
            let left: Vec<&String> = wanted
                .iter()
                .copied()
                .filter(|c| !rows.iter().any(|r| r["property_code"].as_str() == Some(c.as_str())))
                .collect();
            if !left.is_empty() {
                // Alguns imóveis estão em município diferente do que o código indica.
                rows.extend(self.rows(&format!(
                    "SELECT {PROPERTY_FIELDS}, {STAGE} AS etapa FROM {files} WHERE property_code IN ({})",
                    list(&left)
                ))?);
            }
            for row in rows {
                if let Some(code) = row["property_code"].as_str() {
                    found.insert(code.to_string(), row);
                }
            }
        }
        let mut found: HashMap<String, Value> = self
            .with_quality(found.into_values().collect())
            .into_iter()
            .filter_map(|row| row["property_code"].as_str().map(str::to_string).map(|code| (code, row)))
            .collect();
        let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
        let rows: Vec<Value> = codes
            .iter()
            .map(|code| {
                let (check, note, mut row) = if !valid_code(code) {
                    ("invalid", "Texto fora do formato de um código do CAR.".to_string(), json!({"property_code": code}))
                } else if let Some(mut row) = found.remove(code) {
                    let (check, note) = verdict(&row);
                    let (check, note, area) = protected(&row, check, note);
                    row["protected_area"] = area;
                    (check, note, row)
                } else {
                    ("not_found", "Código não encontrado nesta publicação.".to_string(), json!({"property_code": code}))
                };
                *counts.entry(check).or_default() += 1;
                row["check"] = json!(check);
                row["note"] = json!(note);
                row
            })
            .collect();
        self.envelope(fields(vec![("imoveis", json!(rows)), ("resultado", json!(counts))]))
    }
}

// ---- Servidor MCP ----

#[derive(Clone)]
struct Car {
    data: Data,
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
}

impl Car {
    async fn run<F>(&self, tool: &'static str, query: F) -> Result<CallToolResult, McpError>
    where
        F: FnOnce(&Data) -> anyhow::Result<Value> + Send + 'static,
    {
        let data = self.data.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || query(&data))
            .await
            .map_err(|e| McpError::internal_error(e.to_string(), None))?;
        match result {
            Ok(value) => {
                tracing::info!(tool, ms = started.elapsed().as_millis() as u64, "ok");
                Ok(CallToolResult::success(vec![ContentBlock::text(value.to_string())]))
            }
            Err(error) => {
                tracing::warn!(tool, erro = %error, "falhou");
                Ok(CallToolResult::error(vec![ContentBlock::text(error.to_string())]))
            }
        }
    }
}

#[tool_router]
impl Car {
    fn new(data: Data) -> Self {
        Self { data, tool_router: Self::tool_router() }
    }

    #[tool(description = "O que está publicado: UFs, número de imóveis e de municípios, data dos dados.")]
    async fn catalogo(&self) -> Result<CallToolResult, McpError> {
        self.run("catalogo", |d| d.catalogo()).await
    }

    #[tool(description = "O que significa cada coluna dos dados e qual o nome dela no Sicar.")]
    async fn dicionario(&self) -> Result<CallToolResult, McpError> {
        self.run("dicionario", |d| d.dicionario()).await
    }

    #[tool(
        description = "Totais pré-calculados de imóveis e de área declarada (hectares), do país ou de UFs, \
                       por qualquer combinação de UF, situação, porte, tipo, etapa da análise e ano de criação."
    )]
    async fn resumo(&self, Parameters(p): Parameters<Resumo>) -> Result<CallToolResult, McpError> {
        self.run("resumo", move |d| d.resumo(p)).await
    }

    #[tool(
        description = "Quantos imóveis estão em cada etapa da análise, no país ou nas UFs dadas: quantos aguardam \
                       análise, quantos foram notificados e não responderam, quantos têm a análise concluída."
    )]
    async fn etapas_da_analise(&self, Parameters(p): Parameters<Ufs>) -> Result<CallToolResult, McpError> {
        self.run("etapas_da_analise", move |d| d.etapas(p)).await
    }

    #[tool(description = "Os municípios de uma UF, do maior para o menor em imóveis, área declarada, pendentes etc.")]
    async fn municipios(&self, Parameters(p): Parameters<Municipios>) -> Result<CallToolResult, McpError> {
        self.run("municipios", move |d| d.municipios(p)).await
    }

    #[tool(description = "Um município em detalhe: imóveis, área, porte, situação e as etapas da análise.")]
    async fn municipio(&self, Parameters(p): Parameters<Municipio>) -> Result<CallToolResult, McpError> {
        self.run("municipio", move |d| d.municipio(p)).await
    }

    #[tool(
        description = "Os imóveis declarados num ponto (longitude e latitude em graus, SIRGAS 2000). \
                       Mais de um resultado significa sobreposição entre cadastros, que é comum."
    )]
    async fn imoveis_no_ponto(&self, Parameters(p): Parameters<Ponto>) -> Result<CallToolResult, McpError> {
        self.run("imoveis_no_ponto", move |d| d.ponto(p)).await
    }

    #[tool(
        description = "Verifica uma lista de códigos de imóvel do CAR (até 200). Para cada um, o que consta na \
                       camada pública e um resultado: ok (ativo, sem notificação pendente), attention (pendente, \
                       notificado sem resposta, ou com mais de 1% da área sobre terra indígena ou unidade de \
                       conservação de proteção integral), blocked (cancelado ou suspenso), not_found, invalid. \
                       `note` traz o motivo e `protected_area`, os nomes das áreas sobrepostas. A classificação é do \
                       carbr, não uma regra oficial, e não cobre embargos."
    )]
    async fn verificar_imoveis(&self, Parameters(p): Parameters<Codigos>) -> Result<CallToolResult, McpError> {
        self.run("verificar_imoveis", move |d| d.verificar(p)).await
    }
}

#[tool_handler]
impl ServerHandler for Car {
    fn get_info(&self) -> ServerConfig {
        let mut info = ServerConfig::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.instructions = Some(INSTRUCTIONS.into());
        info
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).init();

    let base = std::env::var("CAR_URL").unwrap_or_else(|_| "https://car.castlab.org/current".into());
    let bind = std::env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8765".into());
    let hosts = std::env::var("HOSTS").unwrap_or_else(|_| "mcp.castlab.org,localhost,127.0.0.1".into());

    let data = tokio::task::spawn_blocking(move || Data::open(base)).await??;
    let version = {
        let d = data.clone();
        let cat = tokio::task::spawn_blocking(move || d.catalog()).await??;
        cat["version"].as_str().unwrap_or("?").to_string()
    };

    let config = StreamableHttpServerConfig::default()
        .with_allowed_hosts(hosts.split(',').map(|h| h.trim().to_string()).filter(|h| !h.is_empty()));
    let service = StreamableHttpService::new(
        move || Ok(Car::new(data.clone())),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let app = axum::Router::new()
        .route(
            "/",
            axum::routing::get(|| async {
                "Servidor MCP do CAR (carbr). Conecte um assistente em /mcp. \
                 Documentação: https://car.castlab.org/ajuda.html\n"
            }),
        )
        .route("/healthz", axum::routing::get(|| async { "ok\n" }))
        .nest_service("/mcp", service);

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, versao = %version, "carbr-mcp no ar");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
