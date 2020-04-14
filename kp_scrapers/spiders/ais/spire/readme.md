# [Spire Scraper](www.spire.com)

- [API Documentation](https://spire.com/contact/developer-portal/?access=true)


## Bash Usage

```Bash
readonly SPIRE_BASE_URL="https://ais.spire.com"
# export SPIRE_API_TOKEN="xxxx"

get_messages() {
  declare -r resource="messages"

  curl -XGET --silent \
    -H "Authorization: Bearer ${SPIRE_API_TOKEN}" \
    "${SPIRE_BASE_URL}/${resource}?sort=timestamp&limit=10&fields=decoded" | jq '.'
}
```
