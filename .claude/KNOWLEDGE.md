# База знаний — DM Task Manager

## Деплой на Render

**Проблема:** Изменения видны на localhost, но НЕ видны на https://dm-task-manager.onrender.com/

**Причина:** Render деплоит из Git-репозитория. Пока изменения не запушены — Render показывает старую версию.

**Решение:**
```bash
cd ~/Desktop/dm-task-manager
git add -A
git commit -m "описание изменений"
git push
```
После пуша Render автоматически подхватит и задеплоит (1-2 мин).

**ВАЖНО: ВСЕГДА ДЕЛАТЬ ПУШ ПОСЛЕ ИЗМЕНЕНИЙ!**
Без `git push` изменения видны ТОЛЬКО на localhost. На Render ничего не обновится.

**Порядок работы (ОБЯЗАТЕЛЬНЫЙ):**
1. Внести изменения в код
2. Проверить на localhost:8080
3. **ОБЯЗАТЕЛЬНО:** `git add -A && git commit -m "..." && git push` — деплой на Render
4. Подождать 1-2 минуты
5. Проверить на https://dm-task-manager.onrender.com/

**Быстрая команда (копируй и вставляй):**
```bash
cd ~/Desktop/dm-task-manager && git add -A && git commit -m "update" && git push
```

---

## Перезапуск сервера (localhost)

**Проблема:** `OSError: [Errno 48] Address already in use`

**Решение:**
```bash
lsof -ti:8080 | xargs kill -9; sleep 1; python3 server.py
```

---

## Кэш браузера

**Проблема:** После перезапуска сервера изменения не видны в браузере.

**Решение:** Жёсткое обновление — **Cmd+Shift+R** (Mac) / **Ctrl+Shift+R** (Windows).

---

## Архитектура проекта

- **Сервер:** `server.py` (Python, http.server + SQLite)
- **Фронтенд:** `templates/index.html` (single-page app, vanilla JS)
- **БД:** `task_manager.db` (SQLite)
- **Порт HTTP:** 8080
- **Порт HTTPS:** 8443
- **Хостинг:** Render (auto-deploy from git push)
