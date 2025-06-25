document.addEventListener('DOMContentLoaded', () => {
  const toggleButton = document.getElementById('toggleButton');
  const hiddenContent = document.getElementById('hiddenContent');
  
  toggleButton.addEventListener('click', () => {
    hiddenContent.classList.toggle('mobile-content-visible');
    
    toggleButton.textContent = hiddenContent.classList.contains('mobile-content-visible') 
      ? '<' 
      : '>';
  });
});