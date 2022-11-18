# Processando dados do ENEM com PySpark

### Contexto:
Um cursinho de redação, da cidade de Maceió, que prepara alunos para o ENEM, está querendo aumentar seu engajamento nas redes sociais, para isso criaram uma estratégia de marketing em que desejam ser os primeiros a postarem nas redes sociais a nota de seus alunos, aumentando assim o número de clicks para o perfil do Instagram do cursinho. E além disso, desejam manter salvo a nota da redação e as notas das competências da redação de todos os alunos do cursinho, para futuras análises de evolução do candidato.

O INEP, órgão responsável pelo ENEM, disponibiliza em seu site um arquivo CSV, com diversas informações sobre os participantes, tais como, nota da redação, Município da realização da prova, se o aluno faltou…
Observação: Todos os dados utilizados aqui são públicos disponibilizados no site do INEP. Exceto a coluna "Nome" da planilha "alunos" que gerei de forma automática. O intuito desse projeto é apenas para fins didáticos.

### Missão:
Nossa missão é baixar a base de dados disponibilizada pelo INEP assim que o arquivo for postado no site, filtrar pelos alunos do cursinho e pelo local de prova e armazenar o resultado na nuvem AWS.
Para nos ajudar, o cursinho tem uma tabela em Excel de seus 100 alunos chamada "alunos.xlsx" com o nome dos participantes e o número de sua inscrição na prova do ENEM.

### Tecnologias utilizadas
- Requests
- PySpark
- Pandas
- Boto3
- AWS S3

### Diagrama do projeto
![Diagrama](https://i.imgur.com/NhL57Li.png)

Você pode encontrar como foi realizado esse projeto em detalhes no artigo do Medium: https://medium.com/@gabrielpedrosati/processando-dados-do-enem-com-pyspark-f3627d9af50b

Todos os dados utilizados aqui são públicos, o cursinho e o nome dos alunos são fictícios, projeto desenvolvido apenas para fins didáticos.
