document.addEventListener("DOMContentLoaded", () => {
    const plannedList = document.getElementById("plannedList");
    const currentList = document.getElementById("currentList");
    const bookURL = "/book";

    if (plannedList && typeof exampleBook !== "undefined") {
        for (let i = 0; i < 4; i++) {
            const li = document.createElement("li");
            li.className = "book-list-item";

            const a = document.createElement("a");
            a.href = bookURL;
            a.className = "book-link";
            a.textContent = `${exampleBook.title} — ${exampleBook.author}`;

            li.appendChild(a);
            plannedList.appendChild(li);
        }
    }

    if (currentList && typeof exampleBook !== "undefined") {
        for (let i = 0; i < 7; i++) {
            const li = document.createElement("li");
            li.className = "book-list-item";

            const a = document.createElement("a");
            a.href = bookURL;
            a.className = "book-link";
            a.textContent = `${exampleBook.title} — ${exampleBook.author}`;

            li.appendChild(a);
            currentList.appendChild(li);
        }
    }
});
