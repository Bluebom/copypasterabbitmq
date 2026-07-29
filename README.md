# Copiador de RabbitMQs

Copia N mensagens de cada fila de um RabbitMQ de origem para um RabbitMQ de
destino, sem remover as mensagens originais da origem por padrão (elas são
lidas, republicadas no destino e devolvidas para a origem).

## Instalação

```bash
pip install -r requirements.txt
```

## Configuração

Edite o arquivo `.env` com os dados de conexão da origem e do destino.

- Para copiar só filas específicas, liste-as em `FILAS`, separadas por vírgula.
- `MENSAGENS_POR_FILA` controla quantas mensagens copiar de cada fila.
- `PRESERVAR_ORIGEM=true` devolve a mensagem para a fila de origem depois de
  copiá-la (cópia não destrutiva). `false` remove a mensagem da origem.

## Uso

```bash
python3 main.py
```

## Observações

- As filas já devem existir no destino (o script não redeclara filas, então
  argumentos/políticas customizadas na fila de destino são preservados).
- Quando `PRESERVAR_ORIGEM=true`, a mensagem devolvida reentra no fim da
  fila de origem — a ordem original não é garantida.