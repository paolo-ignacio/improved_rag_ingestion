# Embeddings generation and Pinecone integration
from datetime import datetime
import numpy as np
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec
from config import Settings

settings = Settings()

class VectorService:
    def __init__(self):
        pc = Pinecone(api_key=settings.PINECONE_API_KEY)
        self.index = pc.Index(settings.PINECONE_INDEX_NAME)

        # Remember to pass your actual model string here!
        self.model = SentenceTransformer('sentence-transformers/multi-qa-MiniLM-L6-dot-v1')

    def generate_embeddings(self, text_splits):
        """
        Generates a NumPy array of embeddings for a list of text chunks.
        We keep it as a NumPy array here so .astype() works in the next step.
        """
        try:
            # text_splits can be a single string or a list of strings
            embeddings = self.model.encode(text_splits, batch_size=32, show_progress_bar=False)
            return embeddings
        except Exception as e:
            print(f'Error generating embedding: {str(e)}')
            return None
        
    def upsert_embeddings_to_pinecone(self, embeddings, splits, filename, metadata=None):
        """
        Maps embeddings and text chunks to Pinecone vector payloads and uploads them.
        """
        if metadata is None:
            metadata = {}

        try:
            vectors = []
            upload_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            
            for i, (chunk, embedding) in enumerate(zip(splits, embeddings)):
                flat_metadata = {
                    "source": filename,
                    "chunk_index": i,
                    "text": chunk,
                    "upload_time": upload_time,
                    # Safely unpack file_metadata if it exists inside the passed metadata dict
                    **metadata.get("file_metadata", {}) 
                }
                
                vectors.append({
                    "id": f"{filename}-{i}",
                    # Efficiently cast the individual numpy array chunk and convert to list
                    "values": embedding.astype(np.float32).tolist(),
                    "metadata": flat_metadata
                })

            # Upload to Pinecone in batches of 100
            batch_size = 100
            for i in range(0, len(vectors), batch_size):
                batch = vectors[i:i + batch_size]
                try:
                    # Added 'self.' so it accesses the initialized pinecone index
                    self.index.upsert(vectors=batch)
                    print(f"[VECTOR_SERVICE] Successfully upserted batch {i//batch_size + 1}")
                except Exception as e:
                    print(f"[VECTOR_SERVICE] Pinecone upsert failed for batch starting at index {i}: {str(e)}")
            
            return True

        except Exception as e:
            print(f"[VECTOR_SERVICE] Error in pipeline structural preparation: {str(e)}")
            return False