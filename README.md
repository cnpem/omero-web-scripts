# omero-web-scripts

Esse repositório contém scripts para o OMERO web, que é a interface web do [OMERO](https://www.openmicroscopy.org/omero/).

Consulte a [documentação oficial do OMERO](https://omero.readthedocs.io/en/stable/) para mais detalhes sobre scripting e sua API.

## Testando scripts via CLI

Para testar e executar os scripts, você precisa ter o omero-cli instalado no seu ambiente.

No HPCC Marvin, o omero-cli já está disponível via módulo. Você pode carregar o módulo usando:

```bash
module load omero
```

Para executar um script, use o seguinte comando:

```bash
omero script run <script_name.py>
```

Caso você ainda não tenha feito login com o comando `omero login`, antes de solicitar os parâmetros para execução do script, serão solicitados os dados para login (server, username e senha).

> Você deve passar um caminho relativo ou absoluto para o script, não somente o nome do arquivo. Por exemplo, se você estiver na raiz do repositório, poderá executar o script **Clean_Metadata.py** com:

```bash
omero script run annotation_scripts/Clean_Metadata.py
```

E se já estiver no diretório `annotation_scripts`, poderá executar o script com:

```bash
omero script run ./Clean_Metadata.py
```

## Fazendo upload de scripts para o OMERO

O upload dos scripts para o OMERO pode ser feito a partir da GUI da instância web, mas também pode ser feita via CLI usando o comando `omero script upload`. Para fazer isso, use o seguinte comando:

```bash
omero script upload <script_name.py>
```

Lembre-se que o usuário logado precisa ter permissão para fazer upload de scripts no OMERO.

## Annotation Scripts

São scripts que lidam com anotações, adicionando ou removendo metadados, como tags, key-value pairs, comentários etc. Eles estão localizados no diretório `annotation_scripts`.

- **Clean_Metadata.py**: Remove metadados dos objetos do Omero, como tags, key-value pairs e comentários, com base em critérios específicos. Ele pode ser usado para limpar os metadados, tendo a opção de apagar também os metadados dos objetos filhos.

- **Expand_Metadata.py**: Expande os metadados dos objetos do Omero, copiando os metadados dos objetos pais para os objetos filhos. Ele pode ser usado para garantir que os objetos filhos tenham os mesmos metadados que os objetos pais, facilitando a organização e a busca de dados.
