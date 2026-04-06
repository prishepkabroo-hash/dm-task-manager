#!/bin/bash
# ═══════════════════════════════════════════════════
# Dudarev Motorsport — автоматическая установка
# Запускать на VPS-сервере (Ubuntu 22/24)
# ═══════════════════════════════════════════════════

set -e
echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║  Установка DM Таск-менеджер...           ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""

# 1. Обновление системы
echo "→ Обновляю систему..."
apt update -y && apt upgrade -y

# 2. Установка Python
echo "→ Устанавливаю Python..."
apt install -y python3 python3-pip ufw

# 3. Создание папки проекта
echo "→ Создаю папку проекта..."
mkdir -p /opt/dm-tasks/static
mkdir -p /opt/dm-tasks/templates
mkdir -p /opt/dm-tasks/backups

# 4. Копирование файлов (предполагается что файлы уже загружены в /root/)
if [ -f /root/server.py ]; then
    cp /root/server.py /opt/dm-tasks/
    cp /root/templates/index.html /opt/dm-tasks/templates/
    cp /root/static/logo.jpg /opt/dm-tasks/static/
    echo "→ Файлы скопированы"
else
    echo "→ ВНИМАНИЕ: файлы server.py не найдены в /root/"
    echo "  Загрузите файлы и запустите скрипт снова"
    exit 1
fi

# 5. Создание системного сервиса (автозапуск)
echo "→ Настраиваю автозапуск..."
cat > /etc/systemd/system/dm-tasks.service << 'EOF'
[Unit]
Description=Dudarev Motorsport Task Manager
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/dm-tasks
ExecStart=/usr/bin/python3 /opt/dm-tasks/server.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable dm-tasks
systemctl start dm-tasks

# 6. Открытие порта в фаервол
echo "→ Открываю порт 8080..."
ufw allow 8080/tcp
ufw allow 22/tcp
ufw --force enable

# 7. Настройка ежедневного бэкапа базы данных
echo "→ Настраиваю ежедневный бэкап..."
cat > /opt/dm-tasks/backup.sh << 'BACKUP'
#!/bin/bash
cp /opt/dm-tasks/dm_tasks.db /opt/dm-tasks/backups/dm_tasks_$(date +%Y%m%d_%H%M%S).db
# Удаляем бэкапы старше 30 дней
find /opt/dm-tasks/backups/ -name "*.db" -mtime +30 -delete
BACKUP
chmod +x /opt/dm-tasks/backup.sh

# Добавляем в cron — бэкап каждый день в 3:00
(crontab -l 2>/dev/null; echo "0 3 * * * /opt/dm-tasks/backup.sh") | sort -u | crontab -

echo ""
echo "  ╔══════════════════════════════════════════╗"
echo "  ║  ГОТОВО!                                 ║"
echo "  ║                                          ║"
echo "  ║  Таск-менеджер работает на порту 8080    ║"
echo "  ║  Автозапуск: включён                     ║"
echo "  ║  Бэкап базы: каждый день в 3:00          ║"
echo "  ║                                          ║"
echo "  ║  Откройте: http://IP-СЕРВЕРА:8080        ║"
echo "  ║  Логин: admin / admin123                 ║"
echo "  ╚══════════════════════════════════════════╝"
echo ""
