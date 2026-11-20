<template>
  <section class="page" data-module="recovery_index">
    <header class="page-head">
      <div>
        <h2>任务恢复索引</h2>
        <p class="page-desc">按任务版本定位可恢复检查点并维护任务恢复索引数据表，写入中断后从断点续写，不再从最早检查点扫描全部状态。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记恢复索引</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出恢复索引清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="applyFilters">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <label class="filter-item">
        <span>索引编号</span>
        <input v-model="keyword" data-testid="filter-索引编号" placeholder="按索引编号检索" />
      </label>
      <label class="filter-item">
        <span>状态</span>
        <select v-model="statusFilter" data-testid="filter-status">
          <option value="">全部状态</option>
          <option v-for="s in statuses" :key="s" :value="s">{{ s }}</option>
        </select>
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column" :class="{ sortable: sortableFields.includes(column) }" @click="toggleSort(column)">
            {{ column }}
            <span v-if="sortBy === column">{{ sortOrder === 'asc' ? '▲' : '▼' }}</span>
          </th>
          <th>状态</th>
          <th>关联任务状态</th>
          <th>关联规则版本</th>
          <th>版本一致</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row.status }}</td>
          <td>{{ row['关联任务状态'] ?? '—' }}</td>
          <td>{{ row['关联规则版本'] ?? '—' }}</td>
          <td>{{ row['版本一致'] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in availableActions(row)"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 5" class="empty-state">暂无任务恢复索引数据，可先在流式计算页生成恢复索引</td>
        </tr>
      </tbody>
    </table>

    <div class="pager-bar">
      <button class="btn" type="button" data-testid="prev-page" :disabled="page <= 1" @click="gotoPage(page - 1)">上一页</button>
      <span data-testid="page-info">第 {{ page }} / {{ totalPages }} 页</span>
      <button class="btn" type="button" data-testid="next-page" :disabled="page >= totalPages" @click="gotoPage(page + 1)">下一页</button>
      <label class="filter-item">
        <span>每页条数</span>
        <select v-model.number="size" data-testid="page-size" @change="gotoPage(1)">
          <option :value="10">10</option>
          <option :value="20">20</option>
          <option :value="50">50</option>
        </select>
      </label>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记恢复索引</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" :required="requiredFields.includes(field)" />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条任务恢复索引记录</span>
      <span v-if="actionMessage" class="action-text">{{ actionMessage }}</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/recovery_index'
const columns = ["索引编号", "任务编号", "任务版本", "输出表名", "下一检查点", "检查点时间", "候选检查点数", "来源模块"]
const requiredFields = ["索引编号", "任务编号", "任务版本", "下一检查点"]
const statuses = ["待续写", "可恢复", "已恢复", "已作废"]
const sortableFields = ["索引编号", "任务编号", "任务版本", "输出表名", "下一检查点", "检查点时间"]
const metricNames = ["恢复索引总数", "待续写索引", "可恢复索引"]

const rows = ref<Row[]>([])
const total = ref(0)
const page = ref(1)
const size = ref(10)
const sortBy = ref('')
const sortOrder = ref<'asc' | 'desc'>('asc')
const keyword = ref('')
const statusFilter = ref('')
const errorMessage = ref('')
const actionMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = ["任务编号", "任务版本", "输出表名"]
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})

const totalPages = computed(() => Math.max(1, Math.ceil(total.value / size.value)))
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[0]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[1]).length },
])

function availableActions(row: Row) {
  const actions: string[] = []
  if (row.status === '待续写') actions.push('断点续写')
  if (row.status === '可恢复') actions.push('标记已恢复')
  if (row.status !== '已作废' && row.status !== '已恢复') actions.push('作废索引')
  return actions
}

function buildQuery() {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(filters.value)) {
    if (value) params.set(key, value)
  }
  if (keyword.value) params.set('keyword', keyword.value)
  if (statusFilter.value) params.set('status', statusFilter.value)
  if (sortBy.value) {
    params.set('sort_by', sortBy.value)
    params.set('sort_order', sortOrder.value)
  }
  params.set('page', String(page.value))
  params.set('size', String(size.value))
  return params.toString()
}

function toggleSort(column: string) {
  if (!sortableFields.includes(column)) return
  if (sortBy.value === column) {
    sortOrder.value = sortOrder.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortBy.value = column
    sortOrder.value = 'asc'
  }
  page.value = 1
  void reload()
}

function gotoPage(target: number) {
  page.value = Math.min(Math.max(1, target), totalPages.value)
  void reload()
}

function applyFilters() {
  page.value = 1
  void reload()
}

function resetFilters() {
  filters.value = {}
  keyword.value = ''
  statusFilter.value = ''
  sortBy.value = ''
  sortOrder.value = 'asc'
  page.value = 1
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = buildQuery()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'recovery_index.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  actionMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '任务恢复索引动作未生效')
    actionMessage.value = payload.message || `恢复索引已${action}`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '任务恢复索引操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}?${buildQuery()}`)
    if (!response.ok) {
      throw new Error('恢复索引列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '恢复索引列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.sortable { cursor: pointer; user-select: none; }
.sortable:hover { color: var(--brand); }
.pager-bar { display: flex; gap: 10px; align-items: center; margin-top: 10px; font-size: 13px; color: var(--muted); }
.pager-bar select { padding: 4px 8px; border: 1px solid var(--border); border-radius: 6px; }
.filter-item select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.action-text { color: #15803d; }
</style>
