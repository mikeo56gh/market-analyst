Daily market dashboard quick start

Files in this folder:
- dashboard.html, static page for GitHub Pages
- fetch_data_pack_fixed.py, improved API pull script
- .github/workflows/daily-refresh.yml, daily refresh in GitHub Actions

What to do in GitHub:
1. Create a new repository.
2. Upload every file from this folder to the root of the repo.
3. In GitHub, go to Settings, Secrets and variables, Actions.
4. Add these secrets if you have them:
   - EIA_API_KEY
   - ENTSOE_API_KEY
   - NEWSAPI_KEY
   - GIE_API_KEY
5. Go to Settings, Pages.
6. Under Build and deployment, choose Deploy from a branch.
7. Select main branch and root folder.
8. Save.
9. Open Actions and run Daily refresh once.
10. Open the Pages URL. It will show dashboard.html and read latest.json.

How to refresh every day:
- The workflow is already set to run at 06:35 UTC on weekdays.
- Each run updates latest.json.
- The HTML page reads latest.json when you press Refresh.

Notes:
- NBP, TTF, EU ETS and ethylene still need manual support unless you buy a paid feed.
- If AGSI blocks you, add GIE_API_KEY as a GitHub secret.
- ENTSO-E is now parsed as XML, not JSON.
