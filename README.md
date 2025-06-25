# Prothom Alo Scraper and Viewer

This project scrapes news articles from Prothom Alo, stores them in Elasticsearch, and provides a frontend to view the articles.

## Prerequisites

*   **Docker:** Ensure you have Docker installed and running on your system. You can download it from [https://www.docker.com/get-started](https://www.docker.com/get-started).

## Setup and Execution

Follow these steps to set up and run the project:

### 1. Run Elasticsearch

Open your terminal and run the following command to start Elasticsearch using Docker:

```bash
curl -fsSL https://elastic.co/start-local | sh
```

During the Elasticsearch setup, you will be provided with a password and an API key. **Make sure to save the password, as you will need it in the next step.**

### 2. Configure Backend

You need to update the Elasticsearch password in two files:

*   **`prothom_alo_scraper.py`**:
    *   Open this file in the root directory.
    *   Find the `ES_PASSWORD` variable within the `Config` class.
    *   Replace `"JvQhvZYl"` (or the existing password) with the Elasticsearch password you saved in the previous step.

    ```python
    class Config:
        # ... other configurations ...
        ES_PASSWORD = "YOUR_ELASTICSEARCH_PASSWORD" # <-- UPDATE THIS
        # ...
    ```

*   **`prothomalo_backend/prothomalo_backend/settings.py`**:
    *   Open this file.
    *   Find the `ES_PASSWORD` variable at the bottom of the file.
    *   Replace `"JvQhvZYl"` (or the existing password) with the Elasticsearch password.

    ```python
    # ... other settings ...
    ES_PASSWORD = "YOUR_ELASTICSEARCH_PASSWORD" # <-- UPDATE THIS
    ```

### 3. Run the Scraper (Optional, if you want to fetch new articles)

If you want to scrape the latest articles, run the scraper script.
Open a new terminal in the root directory of the project and execute:

```bash
python prothom_alo_scraper.py
```
This will fetch articles and store them in your Elasticsearch instance.

### 4. Run the Backend Server

Navigate to the backend directory and start the Django development server:

```bash
cd prothomalo_backend
python manage.py runserver
```

The backend server will typically run on `http://127.0.0.1:8000/`.

### 5. Run the Frontend Application

Open another terminal and navigate to the frontend directory:

```bash
cd prothomalo_frontend
```

Install the necessary Node.js packages:

```bash
npm install
```

Then, start the frontend development server:

```bash
npm run dev
```

The frontend application will typically be accessible at `http://localhost:5173/` (Vite's default) or another port specified in the terminal output.

You should now be able to access the Prothom Alo news viewer in your browser.
