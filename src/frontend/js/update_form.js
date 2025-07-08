document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('bookForm');
  const saveButton = document.getElementById('saveButton');
  const ratingInputs = form.querySelectorAll('input[name="rating"]');
  const statusSelect = form.querySelector('#status');
  const hiddenRating = document.getElementById('hiddenRating');

  let ratingChanged = false;
  let statusChanged = false;

  ratingInputs.forEach(input => {
    input.addEventListener('change', () => {
      hiddenRating.value = input.value;
      ratingChanged = true;
      checkAndShowSave();
    });
  });

  statusSelect.addEventListener('change', () => {
    if (statusSelect.value !== "") {
      statusChanged = true;
    } else {
      statusChanged = false;
    }
    checkAndShowSave();
  });

  function checkAndShowSave() {
    if (ratingChanged || statusChanged) {
      saveButton.style.display = 'inline-block';
    } else {
      saveButton.style.display = 'none';
    }
  }

  form.addEventListener('submit', event => {
    event.preventDefault();
    const rating = hiddenRating.value;
    const status = statusSelect.value;

    let messageParts = [];

    if (rating !== "0" && rating !== "") {
      messageParts.push(`Rating: ${rating}`);
    }
    if (status !== "") {
      messageParts.push(`Status: ${status}`);
    }

    const message = messageParts.length > 0 ? `Saved! ${messageParts.join(', ')}` : 'Nothing to save.';

    ratingChanged = false;
    statusChanged = false;
    saveButton.style.display = 'none';
    showNotification(message);
  });

  function showNotification(message) {
    let notif = document.getElementById('saveNotification');
    if (!notif) {
      notif = document.createElement('div');
      notif.id = 'saveNotification';
      notif.style.position = 'fixed';
      notif.style.top = '0';
      notif.style.left = '0';
      notif.style.right = '0';
      notif.style.backgroundColor = '#5300a0';
      notif.style.color = 'white';
      notif.style.height = '35px';
      notif.style.padding = '5px';
      notif.style.textAlign = 'center';
      notif.style.zIndex = '1000';
      notif.style.fontWeight = 'bold';
      document.body.appendChild(notif);
    }
    notif.textContent = message;
    notif.style.display = 'block';

    setTimeout(() => {
      notif.style.display = 'none';
    }, 3000);
  }
});