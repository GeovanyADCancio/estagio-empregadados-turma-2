# Fluxo de trabalho — Secret Scope, Git e Databricks

Este documento registra o fluxo que usamos na dupla para trabalhar com Databricks e Git sem sobrescrever alterações e sem expor credenciais.

## 1. Secret Scope

Usamos o Secret Scope para evitar que os valores reais das credenciais fiquem escritos nos notebooks ou sejam enviados para o Git.

Para que o mesmo notebook funcione nos ambientes dos dois integrantes da dupla, os **nomes dos Secret Scopes e das Keys devem ser iguais**.

Os valores dos secrets são configurados separadamente no ambiente de cada integrante.

### SQL Server

Scope:

```text
mercadata-sqlserver
```

Keys:

```text
jdbc_hostname
jdbc_database
jdbc_username
jdbc_password
```

Exemplo de uso no notebook:

```python
jdbc_password = dbutils.secrets.get(
    scope="mercadata-sqlserver",
    key="jdbc_password"
)
```

### Azure Data Lake

Scope:

```text
mercadata-adls-oauth
```

Keys:

```text
client-id
tenant-id
client-secret
```

Exemplo de uso no notebook:

```python
client_id = dbutils.secrets.get(
    scope="mercadata-adls-oauth",
    key="client-id"
)
```

Assim, o notebook usa a credencial sem armazenar o valor real dela no código.

---

## 2. Cuidados com credenciais

Não colocar os valores reais das credenciais diretamente em:

- notebooks;
- commits;
- README;
- arquivos versionados.

Nos notebooks, as credenciais devem ser recuperadas por meio do Secret Scope, utilizando apenas o nome do scope e da key.

Se um arquivo `.env` for utilizado localmente, ele deve estar no `.gitignore`.

```gitignore
.env
```

---

## 3. Antes de começar a trabalhar

Antes de alterar um notebook, verificamos o estado do repositório e atualizamos a branch:

```bash
git status
git pull --ff-only
```

O `git status` mostra se existem arquivos modificados, novos ou ainda não commitados.

O `git pull --ff-only` traz as alterações do repositório remoto para o ambiente local sem criar um merge automático.

### Quando existem alterações locais

Se houver alterações locais ainda não commitadas, podemos guardá-las temporariamente antes de atualizar a branch:

```bash
git stash push -u -m "backup antes de sincronizar"
git pull --ff-only
```

O `git stash` funciona como um armazenamento temporário das alterações locais.

Para consultar o que está guardado:

```bash
git stash list
```

Caso seja necessário recuperar posteriormente essas alterações:

```bash
git stash pop
```

O `git stash pop` reaplica o stash mais recente no código atual. Como isso pode gerar conflitos caso os mesmos arquivos tenham sido alterados depois do `pull`, deve ser usado com atenção.

Resumindo:

```text
git stash
→ guarda alterações temporariamente

git stash list
→ mostra o que está guardado

git stash pop
→ recupera o que estava guardado
```

---

## 4. Comunicação da dupla

Antes de alterar um notebook compartilhado, avisamos no grupo para evitar que duas pessoas mexam no mesmo arquivo ao mesmo tempo.

Exemplo:

```text
Vou mexer no notebook da Bronze.
Quando fizer o push aviso aqui.
```

Depois do push:

```text
Alterações enviadas.
Pode dar pull.
```

---

## 5. Commit e push

Depois de testar a alteração:

```bash
git status
git add <arquivo>
git commit -m "descrição da alteração"
git push
```

O `push` envia os commits locais para o repositório remoto.


---

## 6. Fluxo resumido

```text
Configurar os Secret Scopes
        ↓
acessar as fontes sem expor credenciais
        ↓
avisar a dupla
        ↓
git status
        ↓
git pull
        ↓
alterar e testar
        ↓
git status
        ↓
commit + push
        ↓
avisar a dupla
```

