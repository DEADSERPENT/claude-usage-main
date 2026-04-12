# Claude Usage Dashboard

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=flat-square)](LICENSE)
[![claude-code](https://img.shields.io/badge/claude--code-black?style=flat-square)](https://claude.ai/code)

🚀 Track, analyze, and optimize your Claude usage — locally and in real-time.

---

## ✨ Features

• Real-time usage tracking (tokens, models, sessions)  
• Cost estimation based on API pricing  
• Local-first (no data leaves your machine)  
• CLI + Web Dashboard  
• Budget protection (alerts + circuit breaker)  
• Zero dependencies — runs with Python only  

---

## 📸 Preview

![Dashboard](docs/screenshot.png)

---

## ⚡ Quick Start

```bash
git clone https://github.com/DEADSERPENT/claude-usage
cd claude-usage

# Scan usage logs
python cli.py scan

# Launch dashboard
python cli.py dashboard
