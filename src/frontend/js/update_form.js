// document.addEventListener("DOMContentLoaded", () => {
// 	const form = document.getElementById("bookForm");
// 	const saveButton = document.getElementById("saveButton");
// 	const ratingInputs = form.querySelectorAll('input[name="rating"]');
// 	const statusSelect = form.querySelector("#status");
// 	const hiddenRating = document.getElementById("hiddenRating");
//
// 	let ratingChanged = false;
// 	let statusChanged = false;
//
// 	ratingInputs.forEach((input) => {
// 		input.addEventListener("change", () => {
// 			hiddenRating.value = input.value;
// 			ratingChanged = true;
// 			checkAndShowSave();
// 		});
// 	});
//
// 	statusSelect.addEventListener("change", () => {
// 		if (statusSelect.value !== "") {
// 			statusChanged = true;
// 		} else {
// 			statusChanged = false;
// 		}
// 		checkAndShowSave();
// 	});
//
// 	function checkAndShowSave() {
// 		if (ratingChanged || statusChanged) {
// 			saveButton.style.display = "inline-block";
// 		} else {
// 			saveButton.style.display = "none";
// 		}
// 	}
//
// 	form.addEventListener("submit", (event) => {
// 		event.preventDefault();
// 		const rating = hiddenRating.value;
// 		const status = statusSelect.value;
//
// 		let messageParts = [];
//
// 		if (rating !== "0" && rating !== "") {
// 			messageParts.push(`Rating: ${rating}`);
// 		}
// 		if (status !== "") {
// 			messageParts.push(`Status: ${status}`);
// 		}
//
// 		const message =
// 			messageParts.length > 0
// 				? `Saved! ${messageParts.join(", ")}`
// 				: "Nothing to save.";
//
// 		ratingChanged = false;
// 		statusChanged = false;
// 		saveButton.style.display = "none";
// 		showNotification(message);
// 	});
// });

function showNotification(message) {
	let notif = document.getElementById("notification");
	notif.textContent = message;
	notif.style.top = "0";

	setTimeout(() => {
		notif.style.top = "-45px";
	}, 3000);
}
