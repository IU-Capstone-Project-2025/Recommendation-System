document.addEventListener('DOMContentLoaded', function () {
    const radioButtons = document.querySelectorAll('input[name="filter"]');
    radioButtons.forEach(radio => {
        radio.addEventListener('change', function () {
            document.getElementById('filterForm').submit();
        });
    });
});