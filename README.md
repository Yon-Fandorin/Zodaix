# Zodaix

Rust 기반의 크로스플랫폼(macOS, Linux) 가상 파일시스템(VFS) 추상화 레이어.

FUSE를 통해 마운트하면 일반 셸에서 접근 가능하여, TUI 환경의 AI 도구(Claude Code 등)가 파일을 자연스럽게 활용할 수 있습니다. 확장 메타데이터(xattr 기반 태그, 설명, 검색 인덱스)를 지원하는 AI 친화적 파일시스템입니다.

## 주요 기능

- **SQLite 백엔드 (기본)** — 영속적 온디스크 저장소. FTS5 전문 검색, 하드링크, 심링크, xattr 지원
- **인메모리 백엔드** — 휘발성 스크래치 공간. 빠른 파일 I/O와 테스트에 적합
- **로컬 패스스루 백엔드** — 실제 디렉토리를 VFS로 마운트하여 메타데이터 레이어 추가
- **FUSE + NFS 이중 트랜스포트** — macFUSE가 설치되어 있으면 FUSE, 없으면 NFSv3로 자동 폴백 (외부 의존성 불필요)
- **xattr 기반 메타데이터** — `user.zodaix.tags`, `user.zodaix.description` 등 확장 속성
- **전문 검색** — SQLite FTS5 기반 전문 검색으로 태그, 설명, 파일명 검색
- **AI 도구 통합** — 특별한 클라이언트 없이 표준 파일시스템 + xattr 명령어로 상호작용

## 요구사항

- Rust 1.70+
- **macOS**: [macFUSE](https://macfuse.github.io/) 또는 [FUSE-T](https://www.fuse-t.org/) (선택사항 — 없으면 NFS로 자동 폴백)
- **Linux**: `libfuse3-dev` (`apt install libfuse3-dev`) (선택사항 — 없으면 NFS로 자동 폴백)

### macFUSE 설치 (macOS, 선택사항)

```bash
brew install --cask macfuse
```

설치 후 **System Settings → Privacy & Security**에서 커널 확장을 허용하고 재부팅합니다.

> **참고**: macFUSE가 설치되어 있지 않아도 Zodaix는 NFSv3 폴백으로 정상 동작합니다.

## 빌드

```bash
git clone https://github.com/Yon-Fandorin/Zodaix.git
cd Zodaix
cargo build --release
```

바이너리: `target/release/zodaix`

## 사용법

### 마운트

```bash
# SQLite 백엔드 (기본값)
zodaix mount /tmp/vfs

# 인메모리 파일시스템
zodaix mount /tmp/vfs --backend memory

# 로컬 디렉토리 패스스루
zodaix mount /tmp/vfs --backend local --root ~/Documents

# 트랜스포트 명시 지정
zodaix mount /tmp/vfs --backend memory --transport fuse   # FUSE 강제
zodaix mount /tmp/vfs --backend memory --transport nfs    # NFS 강제
zodaix mount /tmp/vfs --backend memory --transport auto   # 자동 (기본값)

# NFS 포트 변경 (기본: 11111)
zodaix mount /tmp/vfs --backend memory --transport nfs --nfs-port 22222

# 디버그 로깅
zodaix -v mount /tmp/vfs
```

#### 트랜스포트 선택 동작

| `--transport` | 동작 |
|---|---|
| `auto` (기본값) | FUSE 가용 여부를 감지하여 자동 선택. macFUSE/FUSE-T가 없으면 NFS 폴백 |
| `fuse` | FUSE를 직접 사용. macFUSE/FUSE-T 설치 필요 |
| `nfs` | NFSv3 서버를 localhost에 띄우고 `mount_nfs`로 마운트. 외부 의존성 없음 |

마운트 후 다른 터미널에서:

```bash
ls /tmp/vfs
echo "hello world" > /tmp/vfs/test.txt
cat /tmp/vfs/test.txt
mkdir /tmp/vfs/subdir
cp /tmp/vfs/test.txt /tmp/vfs/subdir/
```

### 언마운트

```bash
zodaix unmount /tmp/vfs
```

또는 마운트 프로세스에서 `Ctrl+C`로 종료합니다 (AutoUnmount 활성화).

### 태그 관리

```bash
zodaix tag add /tmp/vfs/auth.rs security
zodaix tag add /tmp/vfs/auth.rs rust
zodaix tag list /tmp/vfs/auth.rs
zodaix tag remove /tmp/vfs/auth.rs rust
```

### 검색

```bash
zodaix search "auth"
zodaix search "security" --limit 10
```

### 상태 확인

```bash
zodaix status
```

## AI 도구 연동

마운트된 VFS는 일반 파일시스템이므로 별도 클라이언트가 필요 없습니다:

```bash
# xattr로 메타데이터 직접 설정
xattr -w user.zodaix.tags '["auth","security"]' /tmp/vfs/auth.rs
xattr -w user.zodaix.description "인증 로직" /tmp/vfs/auth.rs

# 메타데이터 읽기
xattr -p user.zodaix.tags /tmp/vfs/auth.rs
xattr -p user.zodaix.description /tmp/vfs/auth.rs
```

### xattr 키 규약

| 키 | 형식 | 설명 |
|---|---|---|
| `user.zodaix.tags` | JSON 배열 | 태그 목록 |
| `user.zodaix.description` | UTF-8 문자열 | 파일 설명 |
| `user.zodaix.ai.summary` | UTF-8 문자열 | AI 생성 요약 |
| `user.zodaix.ai.embedding_id` | UTF-8 문자열 | AI 임베딩 식별자 |
| `user.zodaix.mime_type` | UTF-8 문자열 | MIME 타입 |
| `user.zodaix.custom.*` | 임의 | 사용자 정의 속성 |

## 프로젝트 구조

```
Zodaix/
├── crates/
│   ├── core/              # VFS trait, 타입, 에러 정의
│   ├── backends/
│   │   ├── memory/         # 인메모리 백엔드 (DashMap 기반)
│   │   ├── sqlite/         # SQLite 백엔드 (FTS5 검색, 영속 저장)
│   │   └── local/          # 로컬 파일시스템 패스스루
│   ├── metadata/           # 태그 관리 + 검색 인덱스
│   ├── fuse/               # FUSE 브릿지 (fuser → VfsBackend)
│   ├── nfs/                # NFS 브릿지 (nfs3_server → VfsBackend)
│   └── cli/                # CLI 바이너리
└── ref-doc/                # 참조 문서
```

### 크레이트 의존성

```
zodaix (cli)
├── zodaix-core        # VfsBackend trait
├── zodaix-memory      # 인메모리 구현
├── zodaix-sqlite      # SQLite 구현 (FTS5 검색)
├── zodaix-local       # 로컬 FS 구현
├── zodaix-metadata    # 메타데이터 + 검색
├── zodaix-fuse        # FUSE 브릿지
└── zodaix-nfs         # NFS 브릿지
```

## 테스트

```bash
cargo test
```

48개 테스트 포함:
- 메모리 백엔드: 파일 CRUD, 디렉토리, 심링크, 하드링크, xattr, 동시성 (20개)
- SQLite 백엔드: 영속성, 대용량 파일, 하드링크, FTS5 검색, 동시 접근 (26개)
- 메타데이터: 태그 관리 (2개)

## 로드맵

- [x] **v0.1** — Core VFS + FUSE + CLI (메모리/로컬 백엔드)
- [x] **v0.2** — 메타데이터 + 검색
- [x] **v0.2.1** — NFS 폴백 트랜스포트 (FUSE 없이도 마운트 가능)
- [x] **v0.2.2** — SQLite 백엔드 (FTS5 전문 검색, 영속 저장, 기본 백엔드)
- [ ] **v0.3** — Axum 기반 웹 API 서버 + WebSocket 실시간 알림
- [ ] **v0.4** — 웹 UI (파일 브라우저, 태그 관리, 검색)

## 라이선스

[Apache License 2.0](LICENSE)
