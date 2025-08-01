const completed = "completed";
const reading = "reading";
const planned = "planned";
const untracked = "untracked";

function set_score(bookId, score) {
	if (score < 0 || score > 5) {
		return;
	}

	let formData = new FormData();
	formData.append("book_id", bookId);
	formData.append("score", score);

	fetch("/feedback", {
		method: "POST",
		body: formData,
		credentials: "include",
	})
		.then((response) => response.text())
		.then((data) => {
			if (data === "OK") {
				document.getElementById("remove-mark").style.display =
					score != 0 ? "block" : "none";
				document.getElementById(`ratingScore`).innerText = `${score}/5`;
				for (let i = 1; i <= 5; i++) {
					document.getElementById(`rating-${i}`).innerHTML =
						score < i
							? "<i class='bi bi-star'></i>"
							: "<i class='bi bi-star-fill'></i>";
				}
				showNotification("Book rating successfully saved!");
			} else {
				showNotification("Something went wrong :(");
			}
		});
}

function set_status(bookId, status) {
	if (
		status !== completed &&
		status !== reading &&
		status !== planned &&
		status !== untracked
	) {
		return;
	}

	document.getElementById(`status-${status}`).innerHTML =
		"<span class='loader'></span>";

	let formData = new FormData();
	formData.append("book_id", bookId);
	formData.append("status", status);

	fetch("/feedback", {
		method: "POST",
		body: formData,
		credentials: "include",
	})
		.then((response) => response.text())
		.then((data) => {
			if (data === "OK") {
				document.getElementById("status-completed").className = "select-btn";
				document.getElementById("status-reading").className = "select-btn";
				document.getElementById("status-planned").className = "select-btn";
				document.getElementById("status-untracked").className = "select-btn";
				document.getElementById(`status-${status}`).className =
					"select-btn active";
				document.getElementById(`status-${status}`).innerHTML = status;
				showNotification("Book status successfully saved!");
			} else {
				showNotification("Something went wrong :(");
			}
		});
}
