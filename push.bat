@echo off
echo Пушим обновления...

git add -A

for /f "tokens=1-4 delims=/ " %%a in ("%date% %time%") do (
    set commit_msg=Auto update %%a-%%b-%%c_%%d
)

git commit -m "%commit_msg%"

:: подтягиваем изменения с GitHub перед пушем
git pull origin main --rebase

git push origin main

pause
