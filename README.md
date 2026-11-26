# carbr-mcp

Servidor do [Model Context Protocol](https://modelcontextprotocol.io) (MCP) com as consultas ao
**Cadastro Ambiental Rural (CAR)**, em Rust (axum + rmcp), por HTTP.

Endereço público: **`https://mcp.castlab.org/mcp`**

Com ele, um assistente compatível responde perguntas sobre o CAR consultando os dados reais, e cita a
fonte e a data da publicação:

- "Quantos imóveis foram notificados e não responderam em Marabá?"
- "O que está declarado no ponto de longitude -47,9 e latitude -15,8?"
- "Verifique estes códigos de imóvel e diga quais têm pendência."

O servidor não guarda dados. Lê a publicação do [carbr](https://github.com/castlaboratory/carbr-etl)
(GeoParquet em `https://car.castlab.org/current/`) com o DuckDB embutido, só os trechos de que cada
pergunta precisa.

## Conectar um assistente

Clientes que aceitam servidores MCP remotos pedem só o endereço. No Claude Code:

```bash
claude mcp add --transport http car https://mcp.castlab.org/mcp
```

Em clientes configurados por arquivo:

```json
{ "mcpServers": { "car": { "type": "http", "url": "https://mcp.castlab.org/mcp" } } }
```

Para rodar na própria máquina, sem depender deste servidor, o pacote Python traz a mesma lista de
ferramentas pela entrada e saída padrão: `uvx --from "carbr[mcp]" carbr-mcp`.

## Ferramentas

| Ferramenta | O que responde |
|---|---|
| `catalogo` | UFs publicadas, número de imóveis e data dos dados |
| `dicionario` | O que significa cada coluna |
| `resumo` | Totais de imóveis e de área por UF, situação, porte, tipo, etapa e ano de criação |
| `etapas_da_analise` | Quantos imóveis estão em cada etapa da análise, no país ou em UFs |
| `municipios` | Os municípios de uma UF, ordenados por imóveis, área, pendentes etc. |
| `municipio` | Um município em detalhe: etapas da análise, indicadores de qualidade (sobreposição, áreas protegidas, embargos, florestas públicas, Sigef) e o grupo na tipologia |
| `imoveis_no_ponto` | Os imóveis declarados numa coordenada, com aviso de sobreposição |
| `verificar_imoveis` | Triagem de até 200 códigos por chamada: situação, terras indígenas, unidades de conservação, embargos do Ibama, florestas públicas e territórios quilombolas do Sigef |

Toda resposta traz a data da publicação (`versao`) e a citação (`fonte`). Não há ferramenta de
consulta livre: o assistente só faz as perguntas da tabela.

## Rodar

```bash
cargo run --release
```

| Variável | Padrão | Para quê |
|---|---|---|
| `CAR_URL` | `https://car.castlab.org/current` | Endereço da publicação |
| `BIND` | `127.0.0.1:8765` | Endereço de escuta |
| `HOSTS` | `mcp.castlab.org,localhost,127.0.0.1` | Nomes aceitos no cabeçalho `Host` |

Na primeira execução, o DuckDB baixa as extensões `httpfs` e `spatial` do repositório dele.
`GET /healthz` responde `ok`.

## Implantação

`deploy/carbr-mcp.service` é um serviço de usuário do systemd, e `deploy/nginx-mcp.castlab.org.conf`
é o site do nginx que o publica.

```bash
cargo build --release
mkdir -p ~/.config/systemd/user && cp deploy/carbr-mcp.service ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now carbr-mcp
```

O site e o certificado precisam de `sudo`; `deploy/publicar-site.sh` faz os dois e pode ser repetido:

```bash
ssh -tt cast 'bash ~/carbr-mcp/deploy/publicar-site.sh'
```

## Sobre os dados

Fonte: Sicar, Serviço Florestal Brasileiro. O cadastro é autodeclarado: há sobreposições, e constar
no CAR não comprova posse nem propriedade. Republicação independente, sem caráter oficial; em caso de
divergência, vale o Sicar. Não há dados pessoais.

Código sob licença MIT.
