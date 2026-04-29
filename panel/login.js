async function login(password) {
  const response = await fetch('/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    credentials: 'same-origin',
    body: JSON.stringify({ password }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(data.error || `HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
}

const form = document.querySelector('#loginForm');
const errorBox = document.querySelector('#loginError');

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const submit = form.querySelector('button[type="submit"]');
  const passwordInput = form.elements.password;
  const password = new FormData(form).get('password');

  errorBox.textContent = '';
  submit.disabled = true;
  submit.textContent = 'Проверка...';

  try {
    await login(password);
    passwordInput.value = '';
    window.location.replace('/');
  } catch (error) {
    errorBox.textContent = error.status === 429
      ? 'Слишком много попыток. Подожди пару минут.'
      : 'Неверный пароль';
    passwordInput.focus();
  } finally {
    submit.disabled = false;
    submit.textContent = 'Войти';
  }
});
