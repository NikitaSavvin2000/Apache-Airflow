# Infrastructure Apache-Airflow

Шаблон для развертывания Apache Airflow в Docker-контейнерах.

## Структура проекта

### Основные файлы и папки

- **`docker/Dockerfile`**: Dockerfile для сборки образа. Этот файл описывает, как собрать контейнер для Apache Airflow.
- **`docker/docker-compose.yml`**: Конфигурация Docker Compose для упрощенного развертывания и запуска нескольких контейнеров одновременно.
- **`docker/config/tool.yml`**: Основная конфигурация для инструмента, который будет развернут. Этот файл описывает параметры самого инструмента (например, порт, API ключи, зависимости).
- **`.gitignore`**: Файл для игнорирования временных и системных файлов, таких как логи, конфигурации с паролями и файлы PyCharm.
- **`README.md`**: Документация проекта.
- **`src/dags/`**: Папка с DAG-файлами для Apache Airflow.

# Развертывание локально

### Требования

- Docker
- Docker Compose

## Шаги

1. ### Клонируйте репозиторий:

   ```bash
   git clone https://github.com/NikitaSavvin2000/Apache-Airflow.git
   cd Apache-Airflow
   ```

2. ### Запустите контейнеры с помощью Docker Compose:

   ```bash
   docker-compose up -d
   ```

3. ### Перейдите в веб-интерфейс Airflow:
   Откройте [http://localhost:8080](http://localhost:8080) в браузере.

   **Логин:** `admin`  
   **Пароль:** `admin`

4. ### Просмотрите логи контейнеров:
   ```bash
   docker-compose logs
   ```

5. ### Для остановки контейнеров:
   ```bash
   docker-compose down
   ```

# Развертывание на Linux сервере

### Требования

- Docker
- Docker Compose

### Шаги

1. ### Подключитесь к серверу:
   ```bash
   ssh user@your-server-ip
   ```

2. ### Клонируйте репозиторий:
   ```bash
   git clone https://github.com/NikitaSavvin2000/Apache-Airflow.git
   cd Apache-Airflow
   ```

3. ### Запустите контейнеры с помощью Docker Compose:
   ```bash
   sudo docker-compose up -d
   ```

4. ### Перейдите в веб-интерфейс Airflow:
   Откройте `http://your-server-ip:8080` в браузере.

   **Логин:** `admin`  
   **Пароль:** `admin`

5. ### Для остановки контейнеров:
   ```bash
   sudo docker-compose down
   ```

