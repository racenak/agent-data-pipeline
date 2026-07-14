from __future__ import annotations

from openrouter import OpenRouter

from ..config import AppConfig
from ..utils.logger import get_logger

logger = get_logger(__name__)

SYSTEM_PROMPT = """You are an OCR and image analysis assistant. Your task is to:

1. Extract ALL visible text in the image exactly as written
2. Describe any structured data (tables, lists, forms)
3. Note the document type if identifiable (receipt, invoice, letter, screenshot, etc.)
4. Describe non-text visual elements briefly (charts, logos, diagrams)

Format your response as:

## Extracted Text

[all text content here, preserving structure]

## Document Type

[type of document]

## Structured Data

[any tables, lists, or form fields found]

## Visual Elements

[brief description of non-text elements]
"""


class OcrExtractor:
    def __init__(self, config: AppConfig):
        self.config = config
        self._client: OpenRouter | None = None

    def _get_client(self) -> OpenRouter:
        if self._client is None:
            self._client = OpenRouter(api_key=self._resolve_api_key())
        return self._client

    def _resolve_api_key(self) -> str:
        import os

        return os.getenv("OPENROUTER_API_KEY", "")

    def extract(self, image_base64: str, mime_type: str = "image/jpeg") -> str:
        client = self._get_client()
        logger.info("ocr_started", model=self.config.llm.model)

        response = client.chat.send(
            model=self.config.llm.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{image_base64}"
                            },
                        },
                        {
                            "type": "text",
                            "text": "Extract all text and metadata from this image.",
                        },
                    ],
                },
            ],
            max_tokens=self.config.llm.max_tokens,
            temperature=self.config.llm.temperature,
        )

        text = response.choices[0].message.content
        logger.info("ocr_completed", chars=len(text))
        return text
