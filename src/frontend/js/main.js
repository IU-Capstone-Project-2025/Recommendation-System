document.addEventListener("DOMContentLoaded", () => {
    const grid = document.getElementById("bookGrid");

    for (let i = 0; i < 9; i++) {
        const card = document.createElement("div");
        card.className = "book-card";
        card.innerHTML = `
            <img src="${exampleBook.cover}" alt="Book Cover">
            <h3 class="book-title">${exampleBook.title}</h3>
            <p class="book-author">${exampleBook.author}</p>
        `;
        grid.appendChild(card);
    }
});
const filters = document.querySelectorAll(".filter-btn");
filters.forEach(btn => {
    btn.addEventListener("click", () => {
        filters.forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
    });
});
