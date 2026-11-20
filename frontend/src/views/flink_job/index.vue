<template>
  <section class="page" data-module="flink_job">
    <header class="page-head">
      <div><h2>流式计算</h2><p class="page-desc">维护Flink流式任务的SQL逻辑、状态与检查点配置，为流式计算提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记流式任务</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>
    <div class="recovery-panel" data-testid="recovery-panel">
      <h3>任务恢复索引</h3>
      <p class="page-desc">筛选任务版本定位可恢复检查点并生成任务恢复索引数据表；写入中断后从断点续写，保留已完成索引，不再扫描最早检查点。</p>
      <div class="filter-bar">
        <label class="filter-item"><span>流式任务</span>
          <select v-model="recoveryJobId" data-testid="recovery-job" @change="onRecoveryJobChange">
            <option v-for="job in recoveryJobs" :key="job.id" :value="job.id">{{ job.任务编号 }} · {{ job.输出表名 }}</option>
          </select>
        </label>
        <label class="filter-item"><span>任务版本</span>
          <select v-model="recoveryVersion" data-testid="recovery-version">
            <option v-for="v in recoveryVersions" :key="v" :value="v">{{ v }}</option>
          </select>
        </label>
        <label class="filter-item"><span>模拟写入中断</span>
          <input type="checkbox" v-model="simulateInterrupt" data-testid="simulate-interrupt" style="width:auto" />
        </label>
        <button class="btn" type="button" data-testid="load-checkpoints" @click="loadCheckpoints">查询候选检查点</button>
        <button class="btn primary" type="button" data-testid="generate-recovery-index" @click="generateRecoveryIndex">生成恢复索引</button>
      </div>
      <p v-if="recoveryError" class="error-text">{{ recoveryError }}</p>
      <div v-if="checkpointPreview" class="recovery-result" data-testid="checkpoint-preview">
        <p>
          候选检查点：按检查点时间检索 {{ checkpointPreview.time_based_count }} 个，按任务版本检索 {{ checkpointPreview.version_based_count }} 个；
          <template v-if="checkpointPreview.conflict">候选集合冲突，判定按任务版本定位。</template>
          <template v-else>候选集合一致，无需冲突判定。</template>
        </p>
        <table class="data-table">
          <thead><tr><th>检查点编号</th><th>任务版本</th><th>结构版本</th><th>检查点时间</th><th>状态大小</th><th>已写入索引</th></tr></thead>
          <tbody>
            <tr v-for="c in checkpointPreview.checkpoints" :key="c.检查点编号">
              <td>{{ c.检查点编号 }}</td><td>{{ c.任务版本 }}</td><td>{{ c.结构版本 }}</td>
              <td>{{ c.检查点时间 }}</td><td>{{ c.状态大小 }}</td><td>{{ c.已写入索引 }}</td>
            </tr>
            <tr v-if="!checkpointPreview.checkpoints.length"><td colspan="6" class="empty-state">该任务版本下暂无候选检查点</td></tr>
          </tbody>
        </table>
      </div>
      <div v-if="recoveryResult" class="recovery-result" data-testid="recovery-result">
        <p>
          {{ recoveryResult.interrupted ? '写入中断，索引已从断点保留：' : '恢复索引已生成：' }}
          本次写入 {{ recoveryResult.written }} 项，保留已完成 {{ recoveryResult.kept_completed }} 项，
          剩余未完成 {{ recoveryResult.remaining }} 项；下一检查点 {{ recoveryResult.next_checkpoint }}。
        </p>
      </div>
    </div>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记流式任务</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/flink_job'
const columns = ["任务编号", "消费主题", "转换逻辑", "输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]
const actions = ["提交校验", "启动作业", "停止作业"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
const recoveryJobs = ref<any[]>([])
const recoveryJobId = ref<number | null>(null)
const recoveryVersion = ref('')
const simulateInterrupt = ref(false)
const checkpointPreview = ref<any>(null)
const recoveryResult = ref<any>(null)
const recoveryError = ref('')
const recoveryVersions = computed(() => {
  const job = recoveryJobs.value.find((j: any) => j.id === recoveryJobId.value)
  return job ? job.任务版本列表 : []
})
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
async function loadRecoveryJobs() {
  try {
    const res = await request(`${ENDPOINT}/recovery/jobs`)
    const data = await res.json()
    recoveryJobs.value = data.items || []
    if (recoveryJobs.value.length && recoveryJobId.value === null) {
      recoveryJobId.value = recoveryJobs.value[0].id
      const versions = recoveryJobs.value[0].任务版本列表 || []
      recoveryVersion.value = versions[versions.length - 1] || ''
      await loadCheckpoints()
    }
  } catch(e) { recoveryError.value = '恢复索引任务列表加载失败' }
}
async function onRecoveryJobChange() {
  const versions = recoveryVersions.value
  recoveryVersion.value = versions[versions.length - 1] || ''
  recoveryResult.value = null
  await loadCheckpoints()
}
async function loadCheckpoints() {
  recoveryError.value = ''
  if (recoveryJobId.value === null) return
  try {
    const params = new URLSearchParams()
    if (recoveryVersion.value) params.set('任务版本', recoveryVersion.value)
    const res = await request(`${ENDPOINT}/${recoveryJobId.value}/checkpoints?${params}`)
    if (!res.ok) throw new Error('候选检查点读取失败')
    checkpointPreview.value = await res.json()
  } catch(e: any) { recoveryError.value = e.message || '候选检查点读取失败' }
}
async function generateRecoveryIndex() {
  recoveryError.value = ''
  recoveryResult.value = null
  if (recoveryJobId.value === null) { recoveryError.value = '请先选择流式任务'; return }
  if (!recoveryVersion.value) { recoveryError.value = '请先筛选任务版本'; return }
  try {
    const body: Record<string, any> = { 任务版本: recoveryVersion.value }
    if (simulateInterrupt.value) body.fail_after = 1
    const res = await request(`${ENDPOINT}/${recoveryJobId.value}/recovery-index`, {
      method: 'POST', body: JSON.stringify(body),
    })
    const payload = await res.json()
    if (!payload.ok) { recoveryError.value = payload.message || '恢复索引生成失败'; return }
    recoveryResult.value = payload.entry
    await loadCheckpoints()
  } catch(e: any) { recoveryError.value = e.message || '恢复索引生成失败' }
}
onMounted(() => { reload(); loadRecoveryJobs() })
</script>
<style scoped>
.recovery-panel { margin-top: 16px; background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 14px 16px; }
.recovery-panel h3 { margin: 0 0 4px; font-size: 15px; }
.recovery-result { margin-top: 10px; font-size: 13px; }
.filter-item select { padding: 6px 10px; border: 1px solid var(--border); border-radius: 6px; font-size: 13px; }
.error-text { color: #b42318; }
</style>
