# Contribuindo para Dicionários Latim-Português

Obrigado pelo interesse em contribuir com este projeto! Este guia ajudará você a começar.

## 🚀 Configuração do Ambiente de Desenvolvimento

1. **Clone o repositório:**
   ```bash
   git clone https://github.com/Fabio3rs/Dicionarios-Latim.git
   cd Dicionarios-Latim
   ```

2. **Configure um ambiente virtual Python:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   # ou
   .venv\Scripts\activate     # Windows
   ```

3. **Instale dependências de desenvolvimento:**
   ```bash
   make install-dev
   # ou manualmente:
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. **Execute os testes básicos:**
   ```bash
   make test
   ```

## 🔧 Workflow de Desenvolvimento

### Antes de fazer alterações:
```bash
make quality-check  # Verifica formatação, linting, tipos e testes
```

### Durante o desenvolvimento:
```bash
make format      # Formatar código
make lint        # Verificar estilo
make test        # Executar testes
```

## 📝 Diretrizes de Contribuição

### Código
- **Python 3.8+** é obrigatório
- Use **type hints** sempre que possível
- Siga **PEP 8** (verificado automaticamente com `flake8`)
- Formate código com **Black**
- Adicione **logging** para operações importantes
- Trate **erros** adequadamente

### Commits
- Use mensagens descritivas em português ou inglês
- Faça commits pequenos e focados
- Prefixe com o tipo: `feat:`, `fix:`, `docs:`, `refactor:`, etc.

Exemplo:
```
feat: adiciona suporte a busca por regex no query_lexicon
fix: corrige erro de encoding em openai_test.py  
docs: atualiza instruções de instalação
```

### Pull Requests
- Teste suas alterações localmente
- Atualize a documentação se necessário
- Descreva claramente o que foi alterado e por quê

## 🎯 Áreas que Precisam de Ajuda

### Alta Prioridade
- [ ] Testes automatizados mais abrangentes
- [ ] Documentação da API dos módulos
- [ ] Configuração de CI/CD (GitHub Actions)
- [ ] Tratamento de erros mais robusto
- [ ] Performance dos scripts de OCR

### Média Prioridade  
- [ ] Interface web para consultas
- [ ] Suporte a outros formatos de entrada
- [ ] Integração com outros dicionários
- [ ] Ferramentas de validação de dados
- [ ] Métricas de qualidade dos dados

### Baixa Prioridade
- [ ] Containerização (Docker)
- [ ] Suporte multiplataforma melhorado
- [ ] Otimizações de performance
- [ ] Interface gráfica (GUI)

## 🐛 Reportando Problemas

Ao reportar um problema, inclua:

1. **Sistema operacional** e versão do Python
2. **Versão** das dependências (`pip freeze`)
3. **Passos** para reproduzir o erro
4. **Mensagens de erro** completas
5. **Dados de entrada** (se aplicável e não sensível)

## 💡 Sugerindo Melhorias

Para sugestões de funcionalidades:

1. Verifique se não existe uma issue similar
2. Descreva o **problema** que a funcionalidade resolve
3. Explique a **solução proposta**
4. Considere **alternativas**
5. Pense no **impacto** em usuários existentes

## 📚 Estrutura do Projeto

```
├── scripts/           # Pipeline de processamento
│   ├── openai_test.py       # OCR de PDFs
│   ├── analisefaria.py      # Análise e segmentação  
│   ├── openai_parse_mt.py   # Normalização com LLM
│   ├── ingest_normalized.py # Criação do banco
│   └── query_lexicon.py     # Interface de consulta
├── resultados/        # Dados processados
├── dicionarios/       # Recursos externos
├── config.py          # Configurações centralizadas
├── example_usage.py   # Exemplo de uso
└── test_basic.py      # Testes básicos
```

## 🤝 Processo de Review

1. **Fork** o projeto
2. Crie uma **branch** para sua funcionalidade
3. Faça suas **alterações**
4. **Teste** localmente
5. Abra um **Pull Request**
6. Aguarde **review** e feedback

## ❓ Dúvidas

- Abra uma [**Issue**](https://github.com/Fabio3rs/Dicionarios-Latim/issues) para perguntas gerais
- Para dúvidas específicas de código, comente diretamente no código

---

**Obrigado por contribuir!** 🏛️✨