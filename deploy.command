#!/bin/bash
cd "$(dirname "$0")"

echo ""
echo "  =========================================="
echo "  Dudarev Motorsport — Загрузка обновлений"
echo "  =========================================="
echo ""

git add -A
git commit -m "update $(date '+%Y-%m-%d %H:%M')"
git push origin main

echo ""
echo "  ✅ Готово! Render обновит сайт через 2-3 минуты."
echo ""
read -p "  Нажми Enter чтобы закрыть..."
