import json
import hashlib
import re

INPUT_FILE = "data_clean.jsonl"
OUTPUT_FILE = "chunks.jsonl"
MAX_TOKENS = 300
OVERLAP = 50


def classify_section(title):
    if not title:
        return 'other'
    
    title_lower = title.lower()
    
    if 'цел' in title_lower:
        return 'goals'
    elif 'компетенц' in title_lower:
        return 'competencies'
    elif 'результат' in title_lower and 'обучен' in title_lower:
        return 'learning_outcomes'
    elif 'содержан' in title_lower:
        return 'content'
    elif 'фос' in title_lower or 'фонд' in title_lower:
        return 'assessment'
    elif 'литература' in title_lower or 'библиограф' in title_lower:
        return 'bibliography'
    elif 'методическ' in title_lower:
        return 'methodical'
    else:
        return 'other'


def extract_metadata(text, section_title):
    metadata = {
        'has_competencies': bool(re.search(r'УК-\d+|ОПК-\d+|ПК-\d+', text)),
        'has_learning_outcomes': bool(re.search(r'\b(знать|уметь|владеть)\b', text.lower())),
        'has_list': bool(re.search(r'(^\d+\.|^•|^-)', text, re.MULTILINE)),
        'word_count': len(text.split()),
        'section_type': classify_section(section_title),
        'is_substantive': len(text.split()) > 50
    }
    return metadata


def smart_split(text, max_tokens=300, overlap=50):
    paragraphs = text.split('\n\n')
    chunks = []
    current_chunk = []
    current_size = 0
    
    for para in paragraphs:
        words = para.split()
        para_size = len(words)
        
        if para_size > max_tokens:
            if current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = []
                current_size = 0
            
            start = 0
            while start < len(words):
                end = start + max_tokens
                chunk_words = words[start:end]
                chunks.append(' '.join(chunk_words))
                start += max_tokens - overlap
        
        elif current_size + para_size > max_tokens and current_chunk:
            chunks.append('\n\n'.join(current_chunk))
            
            if overlap > 0 and current_chunk:
                overlap_para = current_chunk[-1]
                current_chunk = [overlap_para, para]
                current_size = len(overlap_para.split()) + para_size
            else:
                current_chunk = [para]
                current_size = para_size
        else:
            current_chunk.append(para)
            current_size += para_size
    
    if current_chunk:
        chunks.append('\n\n'.join(current_chunk))
    
    return chunks


def generate_doc_id(source):
    return hashlib.md5(source.encode()).hexdigest()


def main():
    with open(INPUT_FILE, encoding="utf-8") as f:
        records = [json.loads(line) for line in f]

    chunks_out = []
    global_chunk_id = 0

    for record in records:
        text = record["text"]
        source = record["source"]
        section_title = record.get("section_title")
        section_level = record.get("section_level")

        doc_id = generate_doc_id(source)
        section_chunks = smart_split(text, MAX_TOKENS, OVERLAP)

        for idx, chunk in enumerate(section_chunks):
            words = chunk.split()
            word_count = len(words)
            
            if word_count < 30:
                continue
            
            metadata = extract_metadata(chunk, section_title)
            
            chunks_out.append({
                "id": global_chunk_id,
                "doc_id": doc_id,
                "chunk_index": idx,
                "source": source,
                "section_title": section_title,
                "section_level": section_level,
                "text": chunk,
                "metadata": metadata
            })

            global_chunk_id += 1

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for c in chunks_out:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    print(f"Создано чанков: {len(chunks_out)}")


if __name__ == "__main__":
    main()
