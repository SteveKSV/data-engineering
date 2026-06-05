# Лабораторна робота №6 — ETL з Apache Airflow

Простий ETL-пайплайн побудований за допомогою **Apache Airflow** з використанням **TaskFlow API**.

## Структура пайплайну

```
extract() → transform() → load()
```

| Крок | Опис |
|------|------|
| `extract` | Генерує вкладений JSON з 5 записами (metadata + records) |
| `transform` | Розплющує вкладеність — перетворює на плоский список |
| `load` | Створює `pandas.DataFrame` і виводить результат у консоль |

## Структура проєкту

```
Lab_6/
├── dags/
│   └── etl_pipeline.py   # DAG з ETL логікою
├── Dockerfile
├── packages.txt
├── requirements.txt
└── README.md
```

---

## Запуск

### 1. Встановити залежності

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — запустити перед усім іншим
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) для Windows:
```powershell
winget install Astronomer.Astro
```

### 2. Ініціалізувати проєкт

```bash
astro dev init
```

### 3. Покласти DAG у папку `dags/`

Скопіювати `etl_pipeline.py` у папку `dags/` всередині проєкту.

### 4. Запустити Airflow

```bash
astro dev start
```

Після запуску відкрити веб-інтерфейс: [http://localhost:8080](http://localhost:8080)

> Логін: `admin` / Пароль: `admin`

### 5. Протестувати пайплайн

```bash
astro dev run dags test etl_pipeline
```

### 6. Перезапустити (якщо потрібно)

```bash
astro dev restart
```

---

## Технології

- **Apache Airflow 3.x** — оркестрація пайплайну
- **TaskFlow API** (`@dag`, `@task`) — передача даних між задачами через XCom
- **pandas** — формування DataFrame на етапі load
- **Astro CLI** — локальна інфраструктура Airflow
