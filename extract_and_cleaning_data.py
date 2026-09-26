import requests

# Ekstraksi data dari API
query = "Data Engineering"
url = f"https://openlibrary.org/search.json?q={query.replace(' ', '+')}"
response = requests.get(url)
data = response.json()

# Pembersihan dan Transformasi Data
books = []

for book in data['docs'][:10]:
    # print(book.get('title'))
    # print(book.get('author_name')[0])
    # print(book.get('first_publish_year'))
    # print('======')
    books.append({
        'title':book.get('title'),
        'author_name':book.get('author_name')[0],
        'first_publish_year':book.get('first_publish_year')
    })

for book in books:
    print(book)
    print('=====')