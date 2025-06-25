import React from "react";

const NewsCard = ({ news }) => {
  return (
    <div className="border p-4 rounded shadow mb-4">
      <a href={news.url} target="_blank" rel="noopener noreferrer">
        <h2 className="text-xl font-bold text-blue-600">{news.headline}</h2>
      </a>
      <p className="text-gray-600 text-sm">
        {news.author} • {news.location} • {news.published_at}
      </p>
      <p className="mt-2 text-justify">{news.content.slice(0, 300)}...</p>
    </div>
  );
};

export default NewsCard;
