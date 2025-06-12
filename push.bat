@echo off
echo

git add data

for /f "tokens=1-4 delims=/ " %%a in ("%date% %time%") do (
    set commit_msg=Auto update data %%a-%%b-%%c_%%d
)

git commit -m "%commit_msg%"
git push origin main

pause