# Лабораторна робота №3 — Kafka Producer/Consumer - Клем Степан

## Структура проєкту

```
kafka-producer/
├── docker-compose.yml       # Kafka кластер + продюсер
├── producer/
│   ├── Dockerfile           # Образ продюсера
│   ├── producer.py          # Python-продюсер
│   └── requirements.txt
└── data/
    └── Divvy_Trips_2019_Q4.csv   # CSV-файл з даними
```

## Компоненти кластеру

| Сервіс       | Опис                              | Порт       |
|--------------|-----------------------------------|------------|
| zookeeper    | Координатор кластеру              | 2181       |
| broker1      | Kafka брокер 1                    | 9092       |
| broker2      | Kafka брокер 2                    | 9093       |
| kafka-init   | Ініціалізація топіків (одноразово)| —          |
| kafka-ui     | Веб-інтерфейс Kafka UI            | 8080       |
| producer     | Python-продюсер (читає CSV)       | —          |

## Хід роботи

### 1. Підготовка

Перенесення завантаженного CSV-файлу в папку `data/`:

```bash
cp Divvy_Trips_2019_Q4.csv data/
```

### 2. Запуск кластеру та продюсера

```bash
docker-compose up --build
```

### 3. Перевірка через Kafka UI

Відкриваємо браузер: [http://localhost:8080](http://localhost:8080)

- **Topics** → **Topic1** / **Topic2** — перегляд повідомлень
- **Messages** — перегляд окремих записів у форматі JSON

### 4. Зупинка

```bash
docker-compose down
```

Щоб також видалити томи:

```bash
docker-compose down -v
```

## Налаштування продюсера (змінні середовища)

| Змінна                  | За замовчуванням                  | Опис                              |
|-------------------------|-----------------------------------|-----------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `broker1:29092,broker2:29093`   | Адреси брокерів                   |
| `CSV_FILE_PATH`          | `/app/data/Divvy_Trips_2019_Q4.csv` | Шлях до CSV                    |
| `TOPIC1`                 | `Topic1`                          | Назва першого топіку              |
| `TOPIC2`                 | `Topic2`                          | Назва другого топіку              |
| `BATCH_SIZE`             | `500`                             | Кількість записів між flush       |
| `DELAY_SECONDS`          | `0.001`                           | Затримка між батчами (сек)        |

## Формат повідомлення (JSON)

```json
{
  "trip_id": 25223640,
  "start_time": "2019-10-01 00:01:39",
  "end_time": "2019-10-01 00:17:20",
  "bikeid": 2215,
  "tripduration": 940.0,
  "from_station_id": 20,
  "from_station_name": "Sheffield Ave & Kingsbury St",
  "to_station_id": 309,
  "to_station_name": "Leavitt St & Armitage Ave",
  "usertype": "Subscriber",
  "gender": "Male",
  "birthyear": 1987
}
```

Ключ повідомлення: `trip_id` (забезпечує рівномірний розподіл по партиціях).

## Git

```bash
git init
git add .
git commit -m "feat: kafka producer with divvy trips dataset"
git remote add origin <YOUR_REPO_URL>
git push -u origin main
```
