# Function-Stream Client


## Editor Config

### IDEA / WebStorm

* Install plugin `prettier` (may have been pre-installed in webstorm)

* Set package path for `prettier` in IDE's preferences (either in-project or globally installed is ok)

* Set rules as `{**/*,*}.{js,ts,jsx,tsx,vue}`

* Check option `Format on Save`

### VS Code

* Install plugin `Prettier - Code formatter`

* Add the following configs to `.vscode/settings.json`（create if doesn't exist'）
```json
{
  "editor.formatOnSave": true,
  "editor.defaultFormatter": "esbenp.prettier-vscode"
}
```
