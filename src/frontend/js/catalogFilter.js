document.addEventListener("DOMContentLoaded", function () {
    const radios = document.querySelectorAll('input[name="filter"]');
    const title = document.getElementById('section-title');

    const titles = {
        top: "Top",
        weekly: "Popular per week",
        recommend: "Recommendations"
    };

    function updateFromURL() {
        const params = new URLSearchParams(window.location.search);
        const current = params.get('list') || 'top';

        title.textContent = titles[current] || 'Top';

        radios.forEach(r => {
            r.checked = (r.value === current);
        });
    }

    radios.forEach(radio => {
        radio.addEventListener('change', () => {
            const selected = radio.value;

            const url = new URL(window.location);
            url.searchParams.set('list', selected);
            window.history.pushState({}, '', url);

            title.textContent = titles[selected] || 'Top';
        });
    });

    updateFromURL();
});
