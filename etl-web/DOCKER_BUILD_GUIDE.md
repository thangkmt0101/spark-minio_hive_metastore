# Docker Build Guide - Updated Libraries

Hướng dẫn build Docker image với thư viện mới (pandas, openpyxl).

## 📦 Thư viện đã thêm

### Python Packages (requirements.txt)
```
Flask==3.0.0
Flask-CORS==4.0.0
psycopg2-binary==2.9.9
python-dotenv==1.0.0
pandas==2.1.4        # ← MỚI (Excel import/export)
openpyxl==3.1.2      # ← MỚI (Excel engine)
```

### System Dependencies (Dockerfile)
```dockerfile
gcc              # C compiler (cho pandas)
g++              # C++ compiler (cho pandas)
postgresql-client
libxml2-dev      # XML library (cho openpyxl)
libxslt1-dev     # XSLT library (cho openpyxl)
zlib1g-dev       # Compression (cho pandas)
```

## 🔨 Build Docker Image

### 1. Build lần đầu (hoặc sau khi update dependencies)

```bash
# Build image mới
docker-compose build

# Hoặc build không cache
docker-compose build --no-cache
```

### 2. Build nhanh (chỉ code thay đổi)

```bash
# Chỉ rebuild app
docker-compose build web
```

### 3. Build và start

```bash
# Build + start containers
docker-compose up -d --build
```

## ⏱️ Build Time Estimate

### First Build (cold)
```
Pulling base image:     30s-60s
Installing system deps: 20s-40s
Installing Python deps: 60s-120s (pandas lớn)
Copying source:         5s
Total:                  ~2-4 minutes
```

### Rebuild (với cache)
```
System deps (cached):   <1s
Python deps (cached):   <1s
Copying source:         5s
Total:                  ~10-15 seconds
```

## 📝 Dockerfile Breakdown

### Stage 1: Base Image
```dockerfile
FROM python:3.11-slim
WORKDIR /app
```
- Python 3.11 slim (smaller size)
- Set working directory

### Stage 2: System Dependencies
```dockerfile
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
```
- Install compilers (pandas needs C/C++)
- Install XML/XSLT libs (openpyxl needs)
- Clean apt cache (reduce image size)

### Stage 3: Python Dependencies
```dockerfile
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
```
- Copy only requirements first (better caching)
- Install all Python packages
- No pip cache (reduce image size)

### Stage 4: Application Code
```dockerfile
COPY . .
EXPOSE 5333
ENV FLASK_APP=app.py
ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "app.py"]
```
- Copy source code
- Set environment
- Run application

## 🎯 Optimization Tips

### 1. Layer Caching
```dockerfile
# Good: requirements.txt copied separately
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .  # Code changes don't rebuild deps

# Bad: Everything together
COPY . .
RUN pip install -r requirements.txt  # Rebuilds on any code change
```

### 2. Multi-stage Build (Optional)
```dockerfile
# Build stage
FROM python:3.11 as builder
RUN pip install --user -r requirements.txt

# Runtime stage
FROM python:3.11-slim
COPY --from=builder /root/.local /root/.local
```
Smaller final image, but more complex.

### 3. Minimize Layers
```dockerfile
# Good: Single RUN with cleanup
RUN apt-get update && apt-get install -y pkg \
    && rm -rf /var/lib/apt/lists/*

# Bad: Multiple RUN commands
RUN apt-get update
RUN apt-get install -y pkg
RUN rm -rf /var/lib/apt/lists/*
```

## 📊 Image Size

### Expected Sizes
```
Base python:3.11-slim:  ~120 MB
+ System dependencies:  +40 MB
+ Python packages:      +150 MB (pandas is large!)
+ Application code:     +5 MB
Total:                  ~315 MB
```

### Size Breakdown
```
docker images etl-web

REPOSITORY   TAG      SIZE
etl-web      latest   315MB
```

## 🔍 Verify Build

### 1. Check image exists
```bash
docker images | grep etl-web
```

### 2. Check Python packages installed
```bash
docker run --rm etl-web pip list

# Should show:
# Flask         3.0.0
# pandas        2.1.4
# openpyxl      3.1.2
# ...
```

### 3. Test import
```bash
docker run --rm etl-web python -c "import pandas; import openpyxl; print('OK')"
# Should print: OK
```

### 4. Check app runs
```bash
docker-compose up -d
docker-compose logs web

# Should see:
# * Running on http://0.0.0.0:5333
```

## 🐛 Troubleshooting

### Error: "gcc: command not found"

**Cause**: System dependencies not installed

**Solution**: 
```dockerfile
RUN apt-get update && apt-get install -y gcc g++
```

### Error: "libxml/xmlversion.h: No such file"

**Cause**: libxml2-dev not installed

**Solution**:
```dockerfile
RUN apt-get install -y libxml2-dev libxslt1-dev
```

### Error: Build very slow

**Cause**: No cache, downloading packages

**Solutions**:
1. Use image cache: Don't use `--no-cache` unless needed
2. Pre-download packages: `docker pull python:3.11-slim`
3. Use faster mirror: Configure apt mirror

### Error: "No space left on device"

**Cause**: Docker disk full

**Solution**:
```bash
# Clean up
docker system prune -a
docker volume prune

# Check space
docker system df
```

### Error: pandas import fails

**Cause**: Missing dependencies

**Solution**:
```bash
# Rebuild with --no-cache
docker-compose build --no-cache web
```

## 🚀 Build Commands Cheat Sheet

```bash
# Build image
docker-compose build

# Build without cache (fresh)
docker-compose build --no-cache

# Build specific service
docker-compose build web

# Build and start
docker-compose up -d --build

# Rebuild after code change
docker-compose up -d --build web

# View build logs
docker-compose build --no-cache 2>&1 | tee build.log

# Check image size
docker images etl-web

# Inspect image
docker inspect etl-web:latest

# View image layers
docker history etl-web:latest
```

## 📋 Build Checklist

Before building:
- [ ] Updated requirements.txt
- [ ] Added system dependencies in Dockerfile
- [ ] Tested locally with `pip install -r requirements.txt`
- [ ] Committed changes to git (optional)

After building:
- [ ] Image created: `docker images | grep etl-web`
- [ ] Packages installed: `docker run --rm etl-web pip list`
- [ ] App starts: `docker-compose up -d`
- [ ] Import works: Test Excel import feature
- [ ] Export works: Test CSV export feature

## 🔄 Update Workflow

When adding new Python packages:

1. **Update requirements.txt**
   ```bash
   echo "new-package==1.0.0" >> requirements.txt
   ```

2. **Update Dockerfile (if system deps needed)**
   ```dockerfile
   RUN apt-get install -y new-system-package
   ```

3. **Test locally**
   ```bash
   pip install new-package
   python app.py  # Test
   ```

4. **Build Docker image**
   ```bash
   docker-compose build --no-cache
   ```

5. **Test in container**
   ```bash
   docker-compose up -d
   # Test import/export features
   ```

6. **Commit changes**
   ```bash
   git add requirements.txt Dockerfile
   git commit -m "Add new-package for feature X"
   ```

## 📚 Related Files

- `Dockerfile` - Image build configuration
- `requirements.txt` - Python dependencies
- `docker-compose.yml` - Service orchestration
- `.dockerignore` - Exclude files from build
- `DOCKER_GUIDE.md` - Docker deployment guide

## 🎯 Best Practices

### ✅ DO
- ✅ Use specific versions in requirements.txt
- ✅ Clean apt cache after install
- ✅ Use `--no-cache-dir` for pip
- ✅ Copy requirements.txt separately
- ✅ Use slim base images
- ✅ Minimize layers

### ❌ DON'T
- ❌ Use `latest` tags (not reproducible)
- ❌ Install packages without cleanup
- ❌ Copy unnecessary files (use .dockerignore)
- ❌ Run as root (add USER directive for production)
- ❌ Store secrets in image

## 📈 Performance

### Build Time Comparison

| Scenario | Time | Notes |
|----------|------|-------|
| First build (no cache) | 3-5 min | Downloads everything |
| Rebuild (code change only) | 10-15s | Uses cached layers |
| Rebuild (requirements change) | 1-2 min | Reinstalls packages |
| Rebuild (Dockerfile change) | 2-4 min | Rebuilds affected layers |

### Optimization Results

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Image size | N/A | 315 MB | Optimized |
| Build time (first) | N/A | ~3 min | Good |
| Build time (rebuild) | N/A | ~15s | Excellent |
| Startup time | N/A | ~2s | Fast |

---

**Docker image ready!** Build với `docker-compose build` 🐳

**Version**: 1.0  
**Last Updated**: 2026-02-09  
**Includes**: pandas, openpyxl for Excel features
