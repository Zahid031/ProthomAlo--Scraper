import { useEffect, useState } from "react";
import axios from "axios";
import NewsCard from "./components/NewsCard";

function App() {
  const [newsList, setNewsList] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    axios.get("http://127.0.0.1:8000/api/news/")
      .then((response) => {
        setNewsList(response.data);
        setLoading(false);
      })
      .catch((error) => {
        console.error("Error fetching news:", error);
        setLoading(false);
      });
  }, []);

  return (
    <div className="max-w-4xl mx-auto p-6">
      <h1 className="text-3xl font-bold mb-6">📰 Latest News</h1>
      {loading ? (
        <p>Loading...</p>
      ) : (
        newsList.map((news, index) => <NewsCard key={index} news={news} />)
      )}
    </div>
  );
}

export default App;
