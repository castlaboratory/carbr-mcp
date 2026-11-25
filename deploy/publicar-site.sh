#!/usr/bin/env bash
# Publica o servidor MCP em https://mcp.castlab.org: site no nginx e certificado.
# Precisa de sudo. Rode de fora com:  ssh -tt cast 'bash ~/carbr-mcp/deploy/publicar-site.sh'
# Pode ser repetido: cada passo confere o que já existe.
set -euo pipefail

NOME=mcp.castlab.org
ORIGEM="$(cd "$(dirname "$0")" && pwd)/nginx-$NOME.conf"
DISPONIVEL=/etc/nginx/sites-available/$NOME
ATIVO=/etc/nginx/sites-enabled/$NOME

passo() { printf '\n== %s\n' "$1"; }

passo "1/5 O serviço carbr-mcp responde em 127.0.0.1:8765?"
if ! curl -fsS -m 10 http://127.0.0.1:8765/healthz >/dev/null; then
  echo "Não respondeu. Confira: systemctl --user status carbr-mcp"
  echo "Nada foi alterado."
  exit 1
fi
echo "sim"

passo "2/5 Site do nginx"
if [ -f "$DISPONIVEL" ] && grep -q "managed by Certbot" "$DISPONIVEL"; then
  echo "já instalado e com certificado; mantido como está"
else
  sudo cp "$ORIGEM" "$DISPONIVEL"
  echo "copiado para $DISPONIVEL"
fi
[ -L "$ATIVO" ] || sudo ln -s "$DISPONIVEL" "$ATIVO"
echo "ativo em $ATIVO"

passo "3/5 Conferir e recarregar o nginx"
sudo nginx -t
sudo systemctl reload nginx
echo "nginx recarregado"

passo "4/5 Certificado (Let's Encrypt)"
if sudo test -d "/etc/letsencrypt/live/$NOME"; then
  echo "já existe certificado para $NOME"
else
  # Interativo de propósito: o certbot pode perguntar algo; responda no terminal.
  sudo certbot --nginx -d "$NOME"
fi

passo "5/5 Teste"
sleep 2
printf 'por dentro (nginx): '
curl -fsS -m 15 -H "Host: $NOME" http://127.0.0.1/healthz || echo "falhou"
printf 'por fora (https):   '
curl -fsS -m 20 "https://$NOME/healthz" || echo "falhou — se a Cloudflare ainda mostrar a página antiga, espere um minuto e teste de novo"
echo
echo "Pronto. Endereço para os assistentes: https://$NOME/mcp"
