# Function-Stream


## Editor Config

### IDEA / WebStorm

install plugin `prettier` (may have been pre-installed in webstorm)

set package path for `prettier` in IDE's preferences (either in-project or globally installed is ok)

set rules as `{**/*,*}.{js,ts,jsx,tsx,vue}`

check option `Format on Save`

### VS Code

install plugin `Prettier - Code formatter`

add the following configs to `.vscode/settings.json`（create if doesn't exist'）
```json
{
  "editor.formatOnSave": true,
  "editor.defaultFormatter": "esbenp.prettier-vscode"
}
```
