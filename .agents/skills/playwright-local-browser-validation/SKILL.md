---
name: playwright-local-browser-validation
description: Use esta skill quando a tarefa envolver frontend, UI, CSS, layout responsivo, componentes web, rotas locais, bugs visuais, console do navegador, requests de rede, screenshots, Vite, Vue, React, Next, Laravel, Blade, Inertia ou qualquer validação de aplicação web local via Playwright MCP.
---

# Playwright Local Browser Validation

Use esta skill quando a tarefa envolver desenvolvimento, correção, revisão ou validação de interface web local.

A regra central é: não declare uma alteração de frontend como concluída apenas por inspeção estática de código. Sempre que possível, abra a aplicação no navegador via Playwright MCP, colete evidência e corrija os problemas observados.

## Modelo mental

Codex CLI faz:

- leitura de arquivos;
- edição de código;
- execução de comandos shell;
- execução de testes, lint e build;
- inicialização de servidores locais;
- inspeção de imagens locais via `view_image`, quando habilitado.

Playwright MCP faz:

- navegação em navegador;
- snapshot da árvore de acessibilidade;
- interação com elementos;
- coleta de console;
- coleta de requests de rede;
- screenshots;
- resize de viewport;
- espera por texto, carregamento ou estabilização.

## Escopo permitido

Navegue apenas em:

- `http://localhost:*`
- `http://127.0.0.1:*`

Não navegue em domínios externos, exceto quando o usuário pedir explicitamente.

Não imprima segredos de:

- `.env`;
- cookies;
- localStorage;
- sessionStorage;
- headers de autenticação;
- tokens CSRF;
- bearer tokens;
- API keys.

Ao relatar requests de rede, redija ou oculte valores sensíveis.

## Quando ativar

Use esta skill quando:

- a tarefa alterar CSS;
- a tarefa alterar componentes frontend;
- a tarefa alterar templates HTML/Blade/Vue/React/etc.;
- a tarefa alterar rotas visíveis no browser;
- houver bug visual;
- houver problema de botão, formulário, modal, menu, dropdown ou navegação;
- o usuário pedir screenshot;
- o usuário pedir validação mobile/desktop;
- o usuário pedir para rodar `npm run dev`;
- o usuário pedir para abrir localhost;
- o usuário disser que algo “não aparece”, “não clica”, “quebrou o layout” ou “não carrega”.

Não use esta skill para tarefas puramente backend sem efeito visual.

## Workflow obrigatório

### 1. Identificar o projeto

Antes de rodar comandos, inspecione o repositório.

Procure por:

- `package.json`;
- `vite.config.*`;
- `next.config.*`;
- `nuxt.config.*`;
- `artisan`;
- `composer.json`;
- `webpack.config.*`;
- `pnpm-lock.yaml`;
- `yarn.lock`;
- `package-lock.json`;
- `Makefile`;
- arquivos de CI;
- documentação local.

Determine:

- gerenciador de pacote;
- comando de dev server;
- porta provável;
- comando de build;
- comando de lint;
- comando de teste;
- se há backend separado;
- se há frontend separado.

Não invente scripts. Leia os scripts reais do projeto.

### 2. Subir servidor local

Use o comando existente mais adequado.

Exemplos comuns:

- `npm run dev`;
- `pnpm dev`;
- `yarn dev`;
- `npm start`;
- `php artisan serve --host=127.0.0.1 --port=8000`;
- backend + frontend quando necessário.

Sempre que possível, salve logs em `.playwright-mcp/`.

Exemplos:

- `.playwright-mcp/frontend.log`;
- `.playwright-mcp/backend.log`;
- `.playwright-mcp/dev-server.log`.

O servidor não deve bloquear o restante da tarefa. Rode em background, sessão persistente ou padrão equivalente disponível no ambiente.

### 3. Aguardar readiness

Não navegue imediatamente.

Confirme que a aplicação está pronta por um destes métodos:

- mensagem de “ready” no log;
- URL impressa pelo framework;
- `curl -I`;
- `curl`;
- porta respondendo;
- healthcheck local, se existir.

Se o servidor falhar:

- leia o log;
- identifique o erro;
- corrija se estiver no escopo;
- tente novamente;
- se continuar bloqueado, relate o erro exato.

### 4. Abrir no Playwright MCP

Use `browser_navigate` para abrir a URL local.

Após navegar:

1. use `browser_wait_for` para aguardar elemento/texto principal ou estabilização curta;
2. use `browser_snapshot` para obter a árvore acessível;
3. use `browser_console_messages` para coletar erros e warnings relevantes;
4. use `browser_network_requests` para listar requests;
5. use `browser_take_screenshot` para salvar evidência visual.

### 5. Validar desktop

Use viewport desktop:

- `1920x1080`

Com `browser_resize`, ajuste para `1920x1080` quando necessário.

Verifique:

- layout geral;
- header;
- navegação;
- alinhamento;
- espaçamentos;
- overflow horizontal;
- fontes e ícones quebrados;
- assets ausentes;
- conteúdo principal;
- modais/dropdowns quando relevantes;
- mensagens de erro visíveis;
- console errors;
- failed network requests.

Salve screenshot com nome claro, por exemplo:

- `.playwright-mcp/desktop-home.png`;
- `.playwright-mcp/desktop-dashboard.png`;
- `.playwright-mcp/desktop-after-click.png`.

Depois de salvar screenshot, use inspeção de imagem local quando possível, especialmente se a tarefa for visual.

### 6. Validar mobile

Use viewport mobile:

- `375x812`

Com `browser_resize`, ajuste para `375x812`.

Verifique:

- overflow horizontal;
- menu mobile;
- conteúdo cortado;
- botões pequenos demais;
- header fixo;
- footer fixo;
- modais;
- campos de formulário;
- textos sobrepostos;
- cards empilhados;
- comportamento de scroll.

Salve screenshot:

- `.playwright-mcp/mobile-home.png`;
- `.playwright-mcp/mobile-dashboard.png`;
- `.playwright-mcp/mobile-after-click.png`.

Também use inspeção de imagem local quando possível.

### 7. Interagir semanticamente

Prefira `browser_snapshot` para encontrar elementos por nomes acessíveis.

Use:

- `browser_click` para botões, links, abas, menus;
- `browser_type` para digitação simples;
- `browser_fill_form` para formulários;
- `browser_select_option` para selects;
- `browser_press_key` para Enter, Escape, Tab etc.;
- `browser_hover` para menus ou tooltips;
- `browser_wait_for` após ações;
- `browser_handle_dialog` se aparecer alert/confirm/prompt;
- `browser_file_upload` quando a tarefa envolver upload.

Após interações importantes:

- confira `browser_console_messages`;
- confira `browser_network_requests`;
- tire screenshot pós-ação.

### 8. Usar screenshot e view_image

Para tarefas visuais, o ciclo ideal é:

1. Playwright MCP salva screenshot com `browser_take_screenshot`;
2. Codex inspeciona o PNG local com `view_image`, quando disponível;
3. Codex compara o que aparece visualmente com o esperado;
4. Codex corrige CSS/componentes/templates;
5. Codex recarrega a página;
6. Codex tira novo screenshot;
7. Codex repete até a evidência visual estar aceitável.

Use imagem local principalmente para:

- regressão visual;
- alinhamento;
- espaçamento;
- responsividade;
- gráficos;
- canvas;
- mapas;
- ícones;
- componentes sem boa acessibilidade;
- comparação com screenshot enviado pelo usuário.

### 9. Console e network

Sempre leia console depois de abrir a página.

Classifique:

- erros JS;
- warnings relevantes;
- erros de assets;
- erros de hydration;
- erros de Vue/React;
- erros de source map, se irrelevantes, como baixa prioridade.

Sempre leia network quando:

- dados não aparecem;
- API é usada;
- formulário é enviado;
- rota carrega dados;
- assets parecem quebrados;
- há erro visual de imagem/fonte;
- login/sessão interfere.

Use `browser_network_request` para investigar request específico suspeito.

Não exponha headers sensíveis.

### 10. Uso de browser_run_code_unsafe

Evite `browser_run_code_unsafe`.

Use apenas quando:

- não houver alternativa segura;
- for necessário inspecionar estado local da página;
- for necessário reproduzir bug específico;
- a execução estiver limitada ao localhost;
- o código executado for pequeno, legível e justificado.

Nunca use para exfiltrar dados, ler segredos ou contornar segurança.

### 11. Loop de correção

Quando encontrar problema:

1. registre o sintoma;
2. localize arquivo provável;
3. faça patch mínimo;
4. rode build/lint/test quando aplicável;
5. recarregue a página;
6. repita snapshot/console/network/screenshot;
7. confirme desktop e mobile quando o problema for visual.

Não pare no primeiro screenshot se ele mostrar erro evidente.

### 12. Build, lint e testes

Depois das correções, rode os comandos disponíveis no projeto.

Exemplos:

- `npm run lint`;
- `npm test`;
- `npm run test`;
- `npm run build`;
- `pnpm lint`;
- `pnpm test`;
- `pnpm build`;
- `yarn lint`;
- `yarn test`;
- `yarn build`;
- `php artisan test`;
- `composer test`.

Não invente comandos. Leia `package.json`, `composer.json`, Makefile ou CI.

Se um comando falhar por motivo fora do escopo, explique exatamente.

### 13. Resposta final obrigatória

A resposta final do Codex deve incluir:

```text
Arquivos alterados:
- ...

Comandos executados:
- ...

Servidor local:
- comando:
- URL:

Validação no browser:
- URL(s) abertas:
- viewports:
  - 1920x1080
  - 375x812
- screenshots:
  - ...
- console:
  - sem erros relevantes / erros listados
- network:
  - sem falhas relevantes / falhas listadas

Build/lint/test:
- lint:
- tests:
- build:

Problemas encontrados e corrigidos:
- ...

Limitações restantes:
- ...
```

Não dizer apenas “funcionou” ou “looks good”. Sempre entregar evidência.
