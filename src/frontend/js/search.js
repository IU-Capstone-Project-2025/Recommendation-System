document.addEventListener("DOMContentLoaded", function () {
  const searchInput = document.querySelector('.search-input');
  const bookGrid = document.getElementById('bookGrid');

  const mockBooks = [
    { title: "Pride and Prejudice", author: "Jane Austen", cover: "/img/book_cover.jpg" },
    {title: "1984", author: "George Orwell", cover: "/img/book_cover.jpg" },
    {title: "The Great Gatsby", author: "F. Scott Fitzgerald", cover: "/img/book_cover.jpg" }
  ];

  function filterBooks(query) {
    if (!query) return mockBooks;
    return mockBooks.filter(book =>
      book.title.toLowerCase().includes(query.toLowerCase()) ||
      book.author.toLowerCase().includes(query.toLowerCase())
    );
  }

  function renderBooks(books) {
    bookGrid.innerHTML = '';
    books.forEach(book => {
      bookGrid.innerHTML += `
        <div class="book-card">
          <img src="${book.cover}" alt="Book Cover" class="book-cover">
          <h3 class="book-title">${book.title}</h3>
          <p class="book-author">${book.author}</p>
        </div>
      `;
    });
  }

  renderBooks(mockBooks);

  searchInput.addEventListener("input", function () {
    const filteredBooks = filterBooks(this.value);
    renderBooks(filteredBooks);
  });
});