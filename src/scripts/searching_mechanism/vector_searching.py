import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


class BookSearchEngine:
    def __init__(self):
        self.model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        self.book_titles = []
        self.embeddings = None
    
    def load_books(self, filepath: str):
        with open(filepath, 'r', encoding='utf-8') as f:
            self.book_titles = [line.strip() for line in f if line.strip()]
    
        self.embeddings = self.model.encode(self.book_titles, convert_to_tensor=True)
    
    def search(self, query: str, top_k: int = 9):
        query_embedding = self.model.encode(query, convert_to_tensor=True)
        
        query_embedding_np = query_embedding.cpu().numpy().reshape(1, -1)
        embeddings_np = self.embeddings.cpu().numpy()
        
        similarities = cosine_similarity(
            query_embedding_np,
            embeddings_np
        )[0]
        
        top_indices = np.argsort(similarities)[-top_k:][::-1]
        
        return [(self.book_titles[i]) for i in top_indices]

if __name__ == "__main__":
    search_engine = BookSearchEngine()
    search_engine.load_books("titles_only.csv")
    
    while True:
        query = input("temp:")
        results = search_engine.search(query)
        print(results)