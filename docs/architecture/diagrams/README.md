# Diagramas da Arquitetura

Três diagramas obrigatórios para o entregável e para a defesa em banca. Versões em **Mermaid** (renderizam direto no GitHub) servem como fonte de verdade. PNGs de alta resolução serão exportados de Excalidraw/draw.io na S5 (polimento) e adicionados a esta pasta para uso nos slides.

| Diagrama | Audiência | Propósito |
|---|---|---|
| `01-solution.md` | Negócio + banca | Visão de **solução**: o que entra, o que sai, qual o valor |
| `02-data-flow.md` | Banca técnica | Visão de **fluxo de dados**: medalhão Bronze→Silver→Gold + mascaramento |
| `03-deployment.md` | Banca técnica | Visão de **deployment**: containers, redes, dependências |

## Convenções visuais

- **Cores e ícones** padronizados serão aplicados nas versões PNG exportadas.
- **Fontes** sempre à esquerda, **consumo** sempre à direita (fluxo da esquerda para direita).
- **Cross-cutting** (segurança, observabilidade, lineage) aparece como faixa horizontal embaixo, atravessando todas as camadas.
