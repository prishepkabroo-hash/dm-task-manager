# Инструкция для Claude — Проект DM Task Manager

## Обзор проекта

**DM Task Manager (Dudarev Motorsport)** — веб-приложение на Python + vanilla JS.

| Компонент | Технология | Путь |
|-----------|------------|------|
| Сервер | Python `BaseHTTPRequestHandler` | `server.py` |
| Фронтенд | Единый HTML с JS/CSS (~8000+ строк) | `templates/index.html` |
| База данных | SQLite | `dm_tasks.db` |
| Деплой | Render.com (auto-deploy из GitHub) | https://dm-task-manager.onrender.com |

---

## Критические правила (ВСЕГДА соблюдать)

### 1. Деплой ≠ Localhost

Приложение развёрнуто на **Render.com**. Любые изменения в коде нужно:
1. Закоммитить
2. Запушить на GitHub
3. Дождаться деплоя на Render (~2-3 минуты)

**Готовые команды для пользователя:**

```bash
# Перезапуск сервера (давать ВСЕГДА после изменений в server.py)
cd ~/Desktop/dm-task-manager && pkill -f server.py; python3 server.py &

# Коммит и пуш (давать после ВСЕХ изменений)
cd ~/Desktop/dm-task-manager && git add server.py templates/index.html && git push origin main
```

**ВАЖНО:** Всегда давай ГОТОВЫЕ команды, а не инструкции вроде "перезапусти сервер". Пользователь копирует и вставляет в терминал.

### 2. Типы данных из URL

`path.split("/")[N]` возвращает **строку**. Всегда приводить к `int()` для ID:

```python
# ❌ НЕПРАВИЛЬНО
recipient_id = path.split("/")[4]  # строка "5"

# ✅ ПРАВИЛЬНО  
recipient_id = int(path.split("/")[4])  # число 5
```

### 3. Архитектура `do_POST` — порядок handler'ов

В `server.py` метод `do_POST` имеет критический порядок:

```
1. Multipart handlers (avatar, upload) — ДО self._body()
2. data = self._body()              — читает rfile как JSON
3. Все остальные JSON handlers      — ПОСЛЕ self._body()
```

**ЗАПРЕЩЕНО:** Размещать multipart handlers после `self._body()` — `rfile` уже прочитан.

### 4. Кеш браузера

HTML отдаётся с заголовками `no-cache`, но пользователь может видеть старую версию.

**При любых проблемах "ничего не изменилось":**
1. Проверить, перезапущен ли сервер
2. Попросить: **Cmd+Shift+R** (жёсткая перезагрузка)
3. Если не помогает: **Cmd+Shift+N** → `http://localhost:8080` (инкогнито)
4. Если localhost работает, а Render нет — нужен `git push`

### 5. Один файл — один гигантский HTML

Весь фронтенд в `templates/index.html`. При редактировании:
- Всегда проверять синтаксис JS после изменений:
  ```bash
  node -e "const fs=require('fs');const h=fs.readFileSync('templates/index.html','utf8');const s=h.match(/<script[^>]*>([\s\S]*?)<\/script>/gi);s.forEach((x,i)=>{const c=x.replace(/<\/?script[^>]*>/gi,'');try{new Function(c);console.log('OK')}catch(e){console.log('ERROR:',e.message)}});"
  ```
- Проверять Python:
  ```bash
  python3 -c "import py_compile; py_compile.compile('server.py', doraise=True); print('OK')"
  ```

---

## Архитектура мессенджера

### Ключевые функции (фронтенд)

| Функция | Назначение |
|---------|------------|
| `renderMessenger()` | Загружает conversations + groups с сервера, рисует sidebar |
| `_buildConversationListHTML()` | Строит HTML sidebar: dropdown, группы, личные чаты |
| `selectConversation(userId)` | Открывает чат, загружает сообщения, подсвечивает в sidebar |
| `sendDirectMessage()` | Отправляет сообщение, потом `await renderMessenger()` + `await selectConversation()` |
| `deleteConversation()` | Удаляет все сообщения диалога, обновляет sidebar |
| `openGroupChat(groupId)` | Открывает групповой чат |
| `showMsgContextMenu()` | Контекстное меню (ПКМ): реакции, ответ, редактирование, удаление, пересылка |
| `parseUTCDate(str)` | Парсит timestamp из SQLite (UTC без Z) в локальное время |

### Ключевые API endpoints

| Метод | Путь | Назначение |
|-------|------|------------|
| GET | `/api/messenger/conversations` | Список диалогов с last_message |
| GET | `/api/messenger/messages/{userId}` | Сообщения с пользователем |
| POST | `/api/messenger/messages/{userId}` | Отправить сообщение |
| DELETE | `/api/messenger/conversations/{userId}` | Удалить весь диалог |
| DELETE | `/api/messenger/messages/{msgId}` | Soft-delete сообщения |
| PUT | `/api/messenger/messages/{msgId}/edit` | Редактировать сообщение |
| POST | `/api/messenger/reactions` | Добавить/убрать реакцию |
| POST | `/api/messenger/forward` | Переслать сообщение |
| POST | `/api/messenger/mark-read/{userId}` | Отметить прочитанным |
| GET | `/api/messenger/unread` | Количество непрочитанных |
| POST | `/api/messenger/upload` | Загрузка файлов/голосовых |
| GET/POST | `/api/messenger/groups` | Список/создание групп |
| GET/POST | `/api/messenger/groups/{id}/messages` | Сообщения группы |

### Схема таблиц мессенджера

```sql
direct_messages: id, sender_id, recipient_id, text, is_read, created_at, is_deleted, edited_at, reply_to_id, forwarded_from
group_messages: id, group_id, sender_id, text, created_at, is_deleted, edited_at, reply_to_id, forwarded_from
group_chats: id, name, created_by, avatar_color, created_at
group_chat_members: id, group_id, user_id, joined_at
message_reactions: id, message_type, message_id, user_id, emoji, created_at
```

### Форматирование сообщений

Специальные теги в тексте:
- `[img]url[/img]` — изображение
- `[voice]url[/voice]` — голосовое сообщение
- `[file]url[/file]` — файл
- `[reply:name:text]` — ответ
- `[fwd:name]` — пересланное
- `@username` — упоминание (подсвечивается в группах)

---

## Типичные ошибки и как их избежать

### Ошибка: Фото/голосовые не загружаются
**Причина:** Upload handler после `self._body()`.
**Решение:** Upload handler ВСЕГДА перед `data = self._body()` в `do_POST`.

### Ошибка: Диалоги не появляются в sidebar
**Причина:** После отправки не вызывается `renderMessenger()`.
**Решение:** После каждого POST сообщения: `await renderMessenger(); await selectConversation(userId);`

### Ошибка: Dropdown показывает всех пользователей
**Причина:** `conversations` API возвращает пустой массив (сообщения не сохранились).
**Диагностика:** Проверить БД:
```bash
cd ~/Desktop/dm-task-manager && python3 -c "
import sqlite3; conn = sqlite3.connect('dm_tasks.db'); conn.row_factory = sqlite3.Row
msgs = conn.execute('SELECT * FROM direct_messages ORDER BY id DESC LIMIT 10').fetchall()
print(f'Messages: {len(msgs)}')
for m in msgs: print(dict(m))
"
```

### Ошибка: Время отображается неверно
**Причина:** SQLite `CURRENT_TIMESTAMP` = UTC, а JS `new Date("...")` без Z = локальное.
**Решение:** Использовать `parseUTCDate()` вместо `new Date()`.

### Ошибка: Badge непрочитанных не работает
**Причина:** Сервер возвращает `{unread: N}`, а фронт читает `resp.count`.
**Решение:** Всегда проверять имена полей: `resp.unread`.

### Ошибка: Редактирование/удаление в группах не работает
**Причина:** Неверный индекс в `path.split("/")`. Путь: `/api/messenger/groups/123/messages/456/edit` → parts[6] = 456.
**Решение:** Считать сегменты: `['', 'api', 'messenger', 'groups', '{gid}', 'messages', '{mid}', 'edit']`

### Ошибка: "Ничего не изменилось" после правок
**Причина:** Браузер кеширует / Render не обновлён.
**Решение:** 
1. Перезапустить сервер: `cd ~/Desktop/dm-task-manager && pkill -f server.py; python3 server.py &`
2. Жёсткая перезагрузка: Cmd+Shift+R
3. Запушить: `git add server.py templates/index.html && git push origin main`

---

## Стиль работы с пользователем

### ВСЕГДА:
- Давать **готовые команды** для копирования (не "перезапусти сервер", а полную команду с `cd`)
- После изменений в server.py давать команду перезапуска
- После всех изменений давать команду пуша на GitHub
- Проверять синтаксис JS и Python после каждого редактирования
- Проверять БД при проблемах ("данные не сохраняются")
- Добавлять `int()` для всех ID из URL
- Использовать `await` перед async функциями

### НИКОГДА:
- Не размещать multipart handlers после `self._body()`
- Не дублировать код рендеринга (одна функция `_buildConversationListHTML`)
- Не использовать `new Date()` для timestamp из SQLite без `parseUTCDate()`
- Не забывать про Render — localhost ≠ продакшн
- Не менять код без проверки синтаксиса

---

## Быстрые команды

```bash
# Перезапуск сервера
cd ~/Desktop/dm-task-manager && pkill -f server.py; python3 server.py &

# Коммит + пуш
cd ~/Desktop/dm-task-manager && git add server.py templates/index.html && git commit -m "описание" && git push origin main

# Проверка БД
cd ~/Desktop/dm-task-manager && python3 -c "import sqlite3; c=sqlite3.connect('dm_tasks.db'); c.row_factory=sqlite3.Row; [print(dict(r)) for r in c.execute('SELECT * FROM direct_messages ORDER BY id DESC LIMIT 5')]"

# Проверка синтаксиса
cd ~/Desktop/dm-task-manager && python3 -c "import py_compile; py_compile.compile('server.py', doraise=True); print('OK')" && node -e "const h=require('fs').readFileSync('templates/index.html','utf8');const s=h.match(/<script[^>]*>([\s\S]*?)<\/script>/gi);s.forEach((x,i)=>{try{new Function(x.replace(/<\/?script[^>]*>/gi,''));console.log('JS OK')}catch(e){console.log('JS ERROR:',e.message)}});"

# Логи сервера (в реальном времени)
# Просто смотреть терминал где запущен server.py
```
