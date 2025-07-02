document.addEventListener('DOMContentLoaded', function () {
  document.querySelectorAll('.rating input').forEach(input => {
    input.addEventListener('change', function () {
      const selectedValue = this.value;
      document.getElementById('ratingScore').textContent = `${selectedValue}/5`;
    });
  });
});
