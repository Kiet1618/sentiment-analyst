import React, { useState, useEffect } from 'react';
import { Space, Table, Tag, Spin, Alert } from 'antd';
import { fetchSentimentData, SentimentData } from './api';

const SentimentTable: React.FC = () => {
  const [data, setData] = useState<SentimentData[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async (): Promise<void> => {
      try {
        const res: SentimentData[] = await fetchSentimentData();
        setData(res);
      } catch (error) {
        setError('Failed to fetch sentiment data. Please try again.');
      } finally {
        setLoading(false);
      }
    };

    const fetchDataInterval = setInterval(() => {
      fetchData();
    }, 10000); // Fetch data every 10 seconds

    fetchData(); // Initial fetch

    // Cleanup function to clear the interval when the component unmounts
    return () => clearInterval(fetchDataInterval);
  }, []);
  const columns = [
    {
      title: 'Content',
      dataIndex: 'text',
      key: 'text',
      render: (text: string) => <a>{text}</a>,
      width: "60%",
    },
    {
      title: 'Sentiment',
      dataIndex: 'sentiment',
      key: 'sentiment',
      render: (sentiment: string) => {
        let color = '';
        if (sentiment === 'positive') {
          color = 'green'; // Màu xanh lá cho positive
        } else if (sentiment === 'negative') {
          color = 'red'; // Màu đỏ cho negative
        } else {
          color = 'black'; // Màu đen cho neutral
        }

        return <Tag color={color} style={{ width: "100px", textAlign: "center" }}>{sentiment}</Tag>;
      }
    },
    {
      title: 'Positive (%)',
      dataIndex: 'positive_percentage',
      key: 'positive_percentage',
      render: (percentage: number) => <span>{Math.round(percentage)}%</span>,
    },
    {
      title: 'Negative (%)',
      dataIndex: 'negative_percentage',
      key: 'negative_percentage',
      render: (percentage: number) => <span>{Math.round(percentage)}%</span>,
    },
    {
      title: 'Neutral (%)',
      dataIndex: 'neutral_percentage',
      key: 'neutral_percentage',
      render: (percentage: number) => <span>{Math.round(percentage)}%</span>,
    },
    {
      title: 'Compound',
      dataIndex: 'compound',
      key: 'compound',
      render: (compound: number) => <span>{Math.round(compound)}%</span>,
    },
  ];

  return (
    <div>
      {loading && <Spin tip="Loading..." />}
      {error && <Alert message={error} type="error" />}
      {!loading && !error && (
        <Table columns={columns} dataSource={data} />
      )}
    </div>
  );
};

export default SentimentTable;
