# Monitor Hilary Duff

Script em Python que vigia a Ticketmaster Brasil e avisa no celular e no PC quando abre uma data nova do show da Hilary Duff ou quando algum camarote volta a ter ingresso.

## Por que eu fiz isso

A Hilary Duff anunciou um show único no Brasil, dia 13 de agosto de 2027, no Espaço Unimed (São Paulo), pela the lucky me tour. A pré-venda esgotou em uns 40 minutos. No dia 2 de outubro de 2026 abriram uma data extra, 14 de agosto, e quando eu fui olhar o Camarote A/B dessa data também já estava esgotado.

Eu queria camarote. Ficar dando F5 na página o dia inteiro não ia rolar, então montei isso no mesmo dia usando o [Claude Code](https://claude.com/claude-code). A ideia inicial era só saber de uma terceira data com antecedência. Com o monitor rodando, apareceu a outra utilidade: ingresso de camarote volta sozinho de vez em quando, quando alguém desiste, o pagamento falha ou o carrinho expira. Só que some rápido.

O que ele pegou nas primeiras semanas:

- 02/10, 16:21: o camarote 7B do dia 14 liberou. Dois minutos depois já tinha sumido.
- 08/10, 16:35: o dia 13, que estava esgotado, voltou pra venda, com os camarotes 3A, 4B e 7B livres.
- 09/10, 17:59: o camarote 13A do dia 14 liberou, e de novo às 18:06.

Por causa desse primeiro caso de dois minutos, o intervalo de checagem das datas caiu de 2 minutos pra 5 segundos.

## Feito com Claude Code

O projeto inteiro foi feito conversando com o [Claude Code](https://claude.com/claude-code), o assistente de programação da Anthropic. Eu dizia o que queria em português, do jeito que falaria com um amigo ("monitora todas as datas", "só show no Brasil", "a notificação já tem que vir com o link", "45 segundos tá muito tempo"), e o Claude Code fazia o resto: investigou como a página da Ticketmaster entrega os dados, achou as fontes extras (fila virtual, Live Nation, Google News, X), escreveu e testou o script, simulou os alertas antes de ligar de verdade e deixou tudo rodando no meu PC. Os ajustes vieram depois, conforme o monitor ia sendo usado.

## O que ele vigia

Só shows no Brasil. Datas da turnê em outros países, notícias que não citam o Brasil e eventos da Live Nation fora daqui são ignorados.

| Fonte | Frequência | O que procura |
|---|---|---|
| Página de cada data na Ticketmaster | ~5 s | Status da venda e disponibilidade do camarote. Datas novas entram na lista sozinhas |
| Página principal do evento | ~30 s | Botão de data novo, data nova no texto, mudança de status |
| Endereços prováveis de uma nova data | 1 palpite a cada 45 s | A página da data às vezes existe antes de ser linkada no site |
| Fila virtual (Queue-it) com nomes prováveis | 1 palpite a cada 75 s | A fila é criada antes da venda abrir |
| Home da Ticketmaster e Google News | ~5 min | Link novo e manchetes sobre data extra ou terceiro show |
| Página da artista na Live Nation Brasil | ~10 min | Show novo no Brasil |
| Contas no X | 1 conta a cada 30 s | Posts da @LiveNationBR, @TicketmasterBR, @espacounimed, @HilaryDuff e @papelpop |

Abaixo de 5 segundos não adianta: a Ticketmaster serve a página por um cache (CloudFront) que só atualiza a cada 15 s. A checagem de cada data leva uns 0,5 s.

O X não deixa ler nada sem login, então o monitor usa a API pública do [FxTwitter](https://github.com/FixTweet/FxTwitter). Ela lê a timeline de contas específicas (não faz busca por palavra) e falha mais ou menos 1 em cada 4 pedidos. O script tenta de novo, mas é a fonte menos confiável da lista.

## Como ele avisa

Todo alerta leva o link da página de compra da data certa. A Ticketmaster não tem link direto pra um setor, então o link cai na página da data e de lá é clicar em "Ingressos".

| Nível | Quando | O que acontece |
|---|---|---|
| ALTA | Camarote liberou, data nova, venda reabriu | Push urgente no celular, notificação do Windows que fica na tela, janela com som e o navegador abrindo na página |
| MEDIA | Mudanças menores, post sobre as datas que já existem | Push e notificação do Windows |
| BAIXA | "Monitor vivo", uma vez por dia | Só push |

O push vai pelo [ntfy](https://ntfy.sh), que é de graça e não precisa de conta. Tocar na notificação ou no botão "Abrir ingressos" abre a página.

Se a Ticketmaster responder com bloqueio (HTTP 403 ou 429), o monitor desacelera sozinho por 15 minutos. Se ficar mais de 5 minutos sem conseguir abrir as páginas, ele avisa que está "cego".

## Como usar

Precisa de Python 3 no Windows. Não tem dependência: é tudo biblioteca padrão.

1. Copie `config.example.json` para `config.json`.
2. Em `ntfy_topic`, coloque um nome de tópico difícil de adivinhar. Qualquer pessoa que souber o nome consegue ler os seus avisos.
3. Instale o app ntfy no celular e assine o mesmo tópico. No app, libere som pra prioridade máxima e tire o ntfy da economia de bateria.
4. Rode `iniciar_monitor.bat`. Ele sobe em segundo plano, sem janela.

Na primeira rodada ele só registra como as coisas estão (a "base") e não alerta nada. Daí em diante, qualquer mudança gera aviso.

```bash
python monitor.py --status
```

```bash
python monitor.py --test-alert
```

```bash
python monitor.py --stop
```

`--status` mostra o que ele está enxergando agora, `--test-alert` dispara um alerta de teste em todos os canais e `--stop` desliga o monitor. Também dá pra rodar uma única rodada com `--once`.

Pra iniciar junto com o Windows, crie um atalho pra `pythonw.exe monitor.py` na pasta de inicialização (Win+R, `shell:startup`).

O monitor grava três arquivos na própria pasta: `monitor.log` (uma linha de resumo por minuto), `ALERTAS.txt` (histórico de tudo que ele avisou) e `state.json` (a base). Pra recomeçar do zero, é só apagar o `state.json`.

## Ajustes

Qualquer chave do `DEFAULTS` no começo do `monitor.py` pode ser sobrescrita no `config.json`. As mais úteis:

- `watch_sectors`: setores que geram alerta quando ficam disponíveis. O padrão é `["camarote"]`.
- `intervals`: intervalos em segundos de cada checagem.
- `x_accounts`: contas do X vigiadas e o tipo de cada uma (`oficial`, `artista` ou `noticia`).
- `candidate_slugs` e `queue_candidates`: os palpites de endereço e de nome de fila.
- `brazil_terms`: os termos que contam como "fala do Brasil".

Dá pra adaptar pra outro show trocando `landing_url`, `event_slugs`, `artist_regex` e os palpites, mas a leitura da página depende de como a Ticketmaster Brasil monta o HTML hoje. Se mudarem o site, o parser quebra.

## Limites

- Só funciona com o PC ligado e logado.
- A antecedência depende da Ticketmaster. Na data extra de 14/08, anúncio e venda saíram praticamente juntos (a Live Nation postou no X um minuto depois de abrir a venda). Os sinais antecipados, página e fila criadas antes, dependem de acertar o nome, e isso é chute.
- Ele só avisa. Entrar na fila e comprar continua sendo com você.
- Projeto pessoal, sem ligação com Ticketmaster, Live Nation ou a artista.
