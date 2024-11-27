# BreweriesCase
BEES Data Engineering – Breweries Case

O objetivo do projeto é consumir dados de uma API, transformá-los e persisti-los em um data-lake que segue a arquitetura medalhão:

- **Bronze Layer**: Dados brutos.
- **Silver Layer**: Dados transformados e enriquecidos.
- **Gold Layer**: Dados prontos para serem consumidos.

Para isso, utilizamos o **Airflow** como ferramenta de orquestração, o **Docker** para conteinerização e o **MinIO** para persistência dos dados.

---

### Requisitos:

- **Docker Desktop**: [Documentação para instalação](https://docs.docker.com/get-started/get-docker/)
- **Docker Compose Plugin**
- **Airflow in Docker**: [Documentação do Airflow com Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

---

### Como rodar:

1. **Clonar o repositório**:
   ```bash
   git clone https://github.com/Loiana/BreweriesCase.git
   cd BreweriesCase

   
2. **Start Docker**:
   (Com o Docker Desktop em execução)
   ```bash
   docker compose up airflow-init

4. **Access the Airflow interface:**:

   URL: http://localhost:8080

   Username: airflow
   
   Password: airflow

5. **Access the OminIO interface**:

   URL: http://localhost:9001

   Username: ominioadmin
   
   Password: ominioadmin123

6. **Criar os seguintes buckets no OminIO**:

- bronze-layer
- silver_layer
- gold_layer

