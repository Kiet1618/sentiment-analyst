export type SentimentData = {
    text: string;
    negative_percentage: number;
    positive_percentage: number;
    neutral_percentage: number;
    compound: number;
    sentiment: string;

}

export const fetchSentimentData = async (): Promise<Array<SentimentData>> => {
    const response = await fetch('http://localhost:8080');
    console.log(response)
    const data = await response.json();
    return data.reverse();
}