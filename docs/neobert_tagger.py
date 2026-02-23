"""
Классификатор текста по тэгам через NeoBERT.

Использование:
    python neobert_tagger.py --tags tags.txt --text text.txt

Формат tags.txt — один тэг на строку:
    финансы
    спорт
    политика
    технологии

Формат text.txt — просто текст, может быть многострочным.

Дополнительные параметры:
    --top_k 3           показать топ-3 тэга с оценками (по умолчанию 1)
    --model <название>  другая модель с HuggingFace
"""

import argparse
import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel
from pathlib import Path


MODEL_NAME = "chandar-lab/NeoBERT"


def read_tags(path: str) -> list[str]:
    lines = Path(path).read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip()]


def read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def get_embedding(text: str, tokenizer, model) -> torch.Tensor:
    inputs = tokenizer(text, return_tensors="pt", truncation=True, max_length=4096)
    with torch.no_grad():
        outputs = model(**inputs)
    return outputs.last_hidden_state[:, 0, :]  # [CLS] токен


def classify(text: str, tags: list[str], tokenizer, model, top_k: int = 1):
    text_emb = get_embedding(text, tokenizer, model)
    tag_embs = torch.cat([get_embedding(tag, tokenizer, model) for tag in tags])
    similarities = F.cosine_similarity(text_emb, tag_embs)

    scores = similarities.tolist()
    ranked = sorted(
        [{"tag": tag, "score": round(score, 4)} for tag, score in zip(tags, scores)],
        key=lambda x: x["score"],
        reverse=True
    )
    return ranked[:top_k]


def main():
    parser = argparse.ArgumentParser(description="Классификация текста по тэгам через NeoBERT")

    parser.add_argument(
        "--tags",
        type=str,
        required=True,
        help="Путь к файлу с тэгами (один тэг на строку). Пример: --tags tags.txt"
    )
    parser.add_argument(
        "--text",
        type=str,
        required=True,
        help="Путь к файлу с текстом. Пример: --text text.txt"
    )
    parser.add_argument(
        "--top_k",
        type=int,
        default=1,
        help="Сколько лучших тэгов показать (по умолчанию 1)"
    )
    parser.add_argument(
        "--model",
        type=str,
        default=MODEL_NAME,
        help=f"Название модели на HuggingFace (по умолчанию: {MODEL_NAME})"
    )

    args = parser.parse_args()

    tags = read_tags(args.tags)
    text = read_text(args.text)

    print(f"Загружаю модель {args.model}...")
    tokenizer = AutoTokenizer.from_pretrained(args.model, trust_remote_code=True)
    model = AutoModel.from_pretrained(args.model, trust_remote_code=True)
    model.eval()

    print(f"\nТэги ({len(tags)}): {tags}")
    print(f"Текст: {text[:120]}{'...' if len(text) > 120 else ''}\n")

    results = classify(text, tags, tokenizer, model, top_k=args.top_k)

    if args.top_k == 1:
        print(f"Результат: {results[0]['tag']}")
    else:
        print(f"Результат (топ-{args.top_k}):")
        for i, item in enumerate(results, 1):
            print(f"  {i}. {item['tag']:20s} score={item['score']}")


if __name__ == "__main__":
    main()
