const completed = "completed";
const reading = "reading";
const planned = "planned";
const untracked = "untracked";

function set_score(score) {}

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
			}
		});
}
