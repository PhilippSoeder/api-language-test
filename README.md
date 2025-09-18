# API Language Test

This project demonstrates how API responses can be generated based on language preferences. It loads data and descriptions from JSON files, supports multiple languages, and uses a simple caching mechanism.

> **Note:** Loading data and translations directly from files is just for simplicity in this example. In a real API, data would typically be loaded from a database, and translations would be stored on a filestore such as S3.

## How it works

- **main.py**: Core logic for processing requests.
  - Accepts an item ID and an Accept-Language header.
  - **Language negotiation:** The API parses the `Accept-Language` header according to RFC 9110, including support for quality values (`q`). It tries to match the requested language exactly, then falls back to the base language (e.g., `de-CH` → `de`), and finally to a default language if no match is found.
  - Loads data (`data/<id>.json`) and matching descriptions (`i18n/<lang>.json`).
  - Returns a structured response with attributes, values, and language-specific descriptions.
  - **Caching:** Both data and translations are cached in memory using a lazy (on-demand) strategy with a configurable time-to-live (TTL). This reduces file reads and improves performance for repeated requests.

- **i18n/\*.json**: Contains language-specific descriptions for keys.

- **data/\*.json**: Contains the actual data objects with key-value pairs.

- **response_id01_de.json**: Example of an API response.

## Example

A request with `item_id="id01"` and `Accept-Language="de-DE"` returns:

```json
{
  "statusCode": 200,
  "headers": {"Content-Type": "application/json"},
  "body": {
    "id": "id01",
    "language": "de",
    "list-attribute": [
      {
        "key": "key1",
        "description": "Beschreibung 1",
        "value": "value1"
      },
      {
        "key": "key2",
        "description": "Beschreibung 2",
        "value": "value2"
      },
      {
        "key": "key3",
        "description": "Beschreibung 3",
        "value": "value3"
      }
    ]
  }
}
```

## Usage

1. Place the required JSON files in the `data` and `i18n` directories.
2. Run the script with Python:
   ```
   python main.py
   ```
3. Adjust environment variables for language, paths, and cache as needed.

## Customization

- Add new languages by creating additional files in `i18n/`.
- Add new data objects by creating more files in `data/`.

## License

MIT License. See `LICENSE`.
