# Infrastructure Template

Шаблон для развертывания различных инструментов в Docker-контейнерах, с интеграцией с Grafana для мониторинга и сбора метрик.

## Структура проекта

### Основные файлы и папки

- **`docker/Dockerfile`**: Dockerfile для сборки образа. Этот файл описывает, как собрать контейнер для конкретного инструмента.
- **`docker/docker-compose.yml`**: Конфигурация Docker Compose для упрощенного развертывания и запуска нескольких контейнеров одновременно.
- **`docker/config/tool.yml`**: Основная конфигурация для инструмента, который будет развернут. Этот файл описывает параметры самого инструмента (например, порт, API ключи, зависимости).
- **`docker/config/datasources/grafana-datasource.yml`**: Конфигурация источника данных для Grafana, подключающего инструмент для мониторинга.
- **`docker/config/datasources/tool-datasource.yml`**: Конфигурация источника данных для самого инструмента (например, Prometheus или другой мониторинг).
- **`docker-entrypoint.sh`**: Скрипт для старта контейнера и инициализации сервисов.
- **`.gitignore`**: Файл для игнорирования временных и системных файлов, таких как логи, конфигурации с паролями и файлы PyCharm.
- **`README.md`**: Документация проекта.
- **`tutorials/`**: Папка с примерами и инструкциями по настройке и использованию инструментов.

# Развертывание локально

### Требования

- Docker
- Docker Compose

## Шаги

1. ### Клонируйте репозиторий:

   ```bash
   git clone https://github.com/yourusername/infrastructure-template.git
   cd infrastructure-template

2. ### Соберите Docker-образ:

   ```bash
   docker build -t my-tool .

3. ### Запустите контейнеры с помощью Docker Compose:
   ```bash
   docker-compose up -d
4. ### Запустите контейнеры с помощью Docker Compose:
   Перейдите по адресу http://localhost:3000 для доступа к Grafana и настройте источники данных через интерфейс.
5. ### Запустите контейнеры с помощью Docker Compose:
   ```bash
   docker-compose logs
6. ### Для остановки контейнеров:
   ```bash
   docker-compose down


# Развертывание на Linux сервере

### Требования

- Docker
- Docker Compose

### Шаги

1. ### Подключитесь к серверу:

   ```bash
   ssh user@your-server-ip

2. ### Клонируйте репозиторий:
   ```bash
   git clone https://github.com/yourusername/infrastructure-template.git
   cd infrastructure-template
   
3. ### Соберите Docker-образ:
   ```bash
   sudo docker build -t my-tool .

4. ### Запустите контейнеры с помощью Docker Compose:
   ```bash
   sudo docker-compose up -d

5. ### Перейдите на Grafana
   Перейдите по адресу сервера на порту Grafana (например, http://your-server-ip:3000) для настройки и мониторинга.

6. ### Для остановки контейнеров
   ```bash
   sudo docker-compose down
