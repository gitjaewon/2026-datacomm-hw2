# 협업 규칙

HW#2 (Thread Pool 기반 좌석 예매 서버) 팀 작업 규칙입니다.

## 1. 커밋 메시지

### 형식

```
<타입>: <요약>

<본문 (선택)>
```

- 요약은 한 줄, 50자 이내, 마침표 없이
- 본문은 "무엇을, 왜" 바꿨는지 적는다 (어떻게는 코드로 충분)
- 한글/영어 자유, 한 커밋 안에서는 하나로 통일

### 타입

| 타입 | 용도 |
|---|---|
| `feat` | 새 기능 (예: RESERVE_MULTI 처리, Notifier 스레드) |
| `fix` | 버그 수정 |
| `refactor` | 동작 변화 없는 구조 개선 |
| `test` | 테스트/검증 스크립트 |
| `docs` | Readme.txt, AllDefinedLogs.txt 등 문서 |
| `log` | 로그 형식/메시지 추가·변경 |
| `chore` | 빌드 설정, .gitignore 등 기타 |

### 예시

```
feat: RESERVE_MULTI 오름차순 Lock 획득 구현

요청 좌석을 정렬한 뒤 순서대로 Lock을 잡고,
하나라도 EMPTY가 아니면 전부 해제 후 FAIL 응답
```

```
fix: CANCEL 시 Waitlist 맨 앞이 아닌 Client에게 배정되던 문제
```

## 2. 커밋 작성자 / 서명

- 커밋 작성자(author)는 **본인 계정**으로만 한다. 다른 사람 이름으로 커밋하지 않는다.
- 커밋 메시지에 `Co-Authored-By`, `Generated with ...` 같은 **자동 생성 도구의 서명이나 트레일러를 남기지 않는다.** 도구가 자동으로 붙였다면 커밋 전에 지운다.
- 실제로 같이 작업한 팀원이 있을 때만 `Co-Authored-By: 이름 <이메일>`을 붙인다.

## 3. 커밋 단위

- 한 커밋에는 한 가지 변경만 담는다 (기능 + 리팩터링 섞지 않기)
- 빌드가 깨진 상태로 커밋하지 않는다
- 실행 로그(Server.txt, ClientN.txt)는 최종 정식 실행분만 커밋한다. 테스트 중 생긴 로그는 커밋하지 않는다

## 4. 브랜치

- `main`: 항상 빌드·실행 가능한 상태 유지
- 작업은 `feat/<이름>`, `fix/<이름>` 브랜치에서 하고 main에 머지
  - 예: `feat/listener`, `feat/waitlist-notifier`, `fix/socket-send-lock`
- main에 머지하기 전에 팀원 1명 이상 확인

## 5. 코드 공통 규칙 (과제 0점 조건 관련)

머지 전에 아래를 꼭 확인한다.

- [ ] Worker Thread 정확히 10개, 그 외 스레드는 Listener 1 / Notifier 1 / Monitor 1까지만
- [ ] 좌석 접근은 모두 좌석별 Lock 안에서 (전체 Lock 1개 금지)
- [ ] RESERVE_MULTI는 좌석 번호 오름차순으로 Lock 획득, 실패 시 잡은 Lock 전부 해제
- [ ] IP/포트 하드코딩 없음 (실행 인자 또는 설정 파일)
- [ ] 큐 대기는 Condition Variable (sleep 반복 확인 금지)
- [ ] 새 로그 메시지를 추가했다면 AllDefinedLogs.txt도 같이 수정
