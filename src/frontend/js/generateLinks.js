document.addEventListener("DOMContentLoaded", () => {
    const plannedList = document.getElementById("plannedList");
    const currentList = document.getElementById("currentList");
    const bookURL = "/book";

    function createRatingBall(rating) {
        const span = document.createElement("span");
        span.textContent = rating;
        span.style.display = "inline-block";
        span.style.marginLeft = "100px";
        span.style.width = "30px";
        span.style.height = "30px";
        span.style.lineHeight = "30px";
        span.style.textAlign = "center";
        span.style.borderRadius = "50%";
        span.style.backgroundColor = "#5300a0";
        span.style.color = "#fff";
        span.style.fontWeight = "bold";
        span.style.fontSize = "12px";
        return span;
    }

    if (plannedList && typeof exampleBook !== "undefined") {
        for (let i = 0; i < 4; i++) {
            const li = document.createElement("li");
            li.className = "book-list-item";
            li.style.marginBottom = "10px";

            const a = document.createElement("a");
            a.href = bookURL;
            a.className = "book-link";
            a.textContent = `${exampleBook.title} — ${exampleBook.author}`;

            const ratingBall = createRatingBall(exampleBook.rating);

            li.appendChild(a);
            li.appendChild(ratingBall);
            plannedList.appendChild(li);
        }
    }

    if (currentList && typeof exampleBook !== "undefined") {
        for (let i = 0; i < 7; i++) {
            const li = document.createElement("li");
            li.className = "book-list-item";
            li.style.marginBottom = "10px";

            const a = document.createElement("a");
            a.href = bookURL;
            a.className = "book-link";
            a.textContent = `${exampleBook.title} — ${exampleBook.author}`;
            const ratingBall = createRatingBall(exampleBook.rating);
            li.appendChild(a);
            li.appendChild(ratingBall);
            currentList.appendChild(li);
        }
    }
});
