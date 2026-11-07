package io.legado.app.service

import io.legado.app.constant.AppConst
import io.legado.app.constant.AppLog
import io.legado.app.data.AppDbProviders
import io.legado.app.data.entities.Book
import io.legado.app.data.entities.BookChapter
import io.legado.app.data.entities.BookSource
import io.legado.app.help.book.BookHelpProviders
import io.legado.app.help.book.isLocal
import io.legado.app.help.config.AppConfigProviders
import io.legado.app.model.webBook.WebBook
import io.legado.app.utils.HtmlFormatter
import io.legado.app.utils.mapAsync
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.flow

/**
 * 导出书籍正文拼接纯逻辑下沉 (shared commonMain), 供全平台复用。
 *
 * 仅包含不依赖 epublib / OutputStream / Charset 的纯逻辑:
 * - [getAllContents]: 拼接全书正文文本 (供 TXT 导出复用)
 * - [getExportData]: 处理单章正文 (BookHelp.getContent + ContentProcessor)
 *
 * 平台依赖通过 [ExportBookContentDeps] 注入。依赖 epublib (EpubBook) 或
 * OutputStream (TXT 文件写入) 的逻辑见 jvmAndAndroidMain `ExportBookShared`。
 *
 * @see ExportBookContentDeps 平台依赖注入接口
 */
class ExportBookContentShared(private val deps: ExportBookContentDeps) {

    /**
     * 获取全书正文文本 (供 TXT 导出)。
     *
     * 1. 头部拼 "书名\n作者: xxx\n简介: xxx"
     * 2. 并发 (MAX_THREAD) 处理每章 [getExportData]
     * 3. 收集结果按顺序 [append]
     *
     * 章节列表来自 [AppDbProviders.get().bookChapterDao], 已缓存正文来自
     * [BookHelpProviders.get().getContent]。
     */
    suspend fun getAllContents(
        book: Book,
        downloadMissing: Boolean = false,
        append: (text: String) -> Unit
    ) = coroutineScope {
        val useReplace = deps.exportUseReplace && book.getUseReplaceRule()
        val qy = "${book.name}\n${
            deps.strAuthorShow(book.getRealAuthor())
        }\n${
            deps.strIntroShow(
                "\n" + HtmlFormatter.format(book.getDisplayIntro())
            )
        }"
        append(qy)
        val chapters = ensureExportChapterList(book, downloadMissing)
        if (downloadMissing && chapters.isEmpty()) error("没有章节，无法下载整本 TXT")
        val bookSource = if (downloadMissing && !book.isLocal) {
            AppDbProviders.get().bookSourceDao.getBookSource(book.origin)
                ?: error("书源已删除或失效，无法下载《${book.name}》")
        } else {
            null
        }
        flow {
            chapters.forEachIndexed { index, chapter ->
                emit(index to chapter)
            }
        }.mapAsync(
            if (downloadMissing) {
                AppConfigProviders.get().threadCount.coerceIn(1, AppConst.MAX_THREAD)
            } else {
                AppConst.MAX_THREAD
            }
        ) { (index, chapter) ->
            getExportData(
                book = book,
                chapter = chapter,
                nextChapterUrl = chapters.getOrNull(index + 1)?.url,
                useReplace = useReplace,
                downloadMissing = downloadMissing,
                bookSource = bookSource,
            )
        }.collectIndexed { index, result ->
            deps.postExportEvent(book.bookUrl)
            deps.setExportProgress(book.bookUrl, index)
            append.invoke(result)
        }
    }

    /**
     * 处理单章正文 (BookHelp.getContent + ContentProcessor.getContent)。
     *
     * - 读取已缓存正文 (BookHelp.getContent); 缺失时写空正文 (不再写 "null" 字面量,
     *   否则导出成品在阅读器里显示"正文=null")
     * - ContentProcessor 处理 (不导出 vip 标识, 是否含标题由
     *   [ExportBookContentDeps.exportNoChapterName] 决定, 不做简繁/重排)
     * - 返回 "\n\n" + 处理后正文
     *
     * @param useReplace 是否应用替换规则 (与 [Book.getUseReplaceRule] 取 AND 后传入)
     */
    private suspend fun getExportData(
        book: Book,
        chapter: BookChapter,
        nextChapterUrl: String?,
        useReplace: Boolean,
        downloadMissing: Boolean,
        bookSource: BookSource?,
    ): String {
        val content = getExportRawContent(
            book = book,
            chapter = chapter,
            nextChapterUrl = nextChapterUrl,
            downloadMissing = downloadMissing,
            bookSource = bookSource,
        )
        // 缺章写空正文并留痕 (对齐 ExportBookEpubShared.missingChapterContent):
        // 写 "null" 字面量会让导出成品在阅读器里显示“正文=null”
        if (content == null && !chapter.isVolume) {
            AppLog.put("导出: 章节正文为空, 已写空正文 (${book.name} / ${chapter.title})")
        }
        val content1 = deps.processContent(
            book,
            // 不导出vip标识
            chapter.apply { isVip = false },
            content ?: "",
            includeTitle = !deps.exportNoChapterName,
            useReplace = useReplace,
            chineseConvert = false,
            reSegment = false
        ).toString()
        return "\n\n$content1"
    }
}

/** 下载整本前保证目录已经落库；普通导出不触发网络请求。 */
suspend fun ensureExportChapterList(
    book: Book,
    downloadMissing: Boolean,
): List<BookChapter> {
    val appDb = AppDbProviders.get()
    var chapters = appDb.bookChapterDao.getChapterList(book.bookUrl)
    if (chapters.isNotEmpty() || !downloadMissing || book.isLocal) return chapters

    val bookSource = appDb.bookSourceDao.getBookSource(book.origin)
        ?: error("书源已删除或失效，无法下载《${book.name}》")
    if (book.tocUrl.isEmpty()) {
        WebBook.getBookInfoAwait(bookSource, book)
    }
    chapters = WebBook.getChapterListAwait(bookSource, book).getOrThrow()
    if (chapters.isEmpty()) error("没有章节，无法下载整本 TXT")
    appDb.bookChapterDao.insert(*chapters.toTypedArray())
    appDb.bookDao.update(book)
    return chapters
}

/**
 * 读取导出所需的章节正文。
 *
 * 普通“导出”保持原行为，只读取本地缓存；“下载整本 TXT”会在缓存未命中时按书源规则
 * 联网获取正文。WebBook 默认会把成功结果写入章节缓存，因此后续阅读和再次导出可直接复用。
 * 单章最多尝试三次，最终仍失败则抛错，由上层删除未完成的 TXT，避免生成缺章文件。
 */
suspend fun getExportRawContent(
    book: Book,
    chapter: BookChapter,
    nextChapterUrl: String?,
    downloadMissing: Boolean,
    bookSource: BookSource? = null,
): String? {
    BookHelpProviders.get().getContent(book, chapter)?.let { return it }
    if (!downloadMissing || book.isLocal || chapter.isVolume) return null

    val source = bookSource ?: AppDbProviders.get().bookSourceDao.getBookSource(book.origin)
        ?: error("书源已删除或失效，无法下载《${book.name}》")
    var lastError: Throwable? = null
    repeat(3) { attempt ->
        try {
            return WebBook.getContentAwait(
                bookSource = source,
                book = book,
                bookChapter = chapter,
                nextChapterUrl = nextChapterUrl,
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            lastError = e
            if (attempt < 2) delay((attempt + 1) * 1000L)
        }
    }
    throw IllegalStateException(
        "下载章节失败：${chapter.title}${lastError?.message?.let { "\n$it" }.orEmpty()}",
        lastError,
    )
}

/**
 * 导出书籍正文拼接平台依赖注入接口 (commonMain)。
 *
 * [ExportBookContentShared] 所需依赖子集, 各平台实现并注入。
 * jvmAndAndroidMain 的 `ExportBookDeps` 继承本接口, 复用同一份 deps 实现。
 */
interface ExportBookContentDeps {

    /** 是否启用替换规则 (对应 AppConfig.exportUseReplace)。 */
    val exportUseReplace: Boolean

    /** 是否不导出章节名 (对应 AppConfig.exportNoChapterName)。 */
    val exportNoChapterName: Boolean

    /** 格式化作者行 (对应 getString(R.string.author_show, author))。 */
    fun strAuthorShow(author: String): String

    /** 格式化简介行 (对应 getString(R.string.intro_show, intro))。 */
    fun strIntroShow(intro: String): String

    /**
     * 走完整正文处理 (替换规则 / 简繁 / 重排段 / 去重复标题),
     * 对应 `ContentProcessor.get(book).getContent(book, chapter, content, ...)`。
     */
    fun processContent(
        book: Book,
        chapter: BookChapter,
        content: String,
        includeTitle: Boolean,
        useReplace: Boolean,
        chineseConvert: Boolean,
        reSegment: Boolean
    ): CharSequence

    /** 发布导出事件 (对应 postEvent(EventBus.EXPORT_BOOK, bookUrl))。 */
    fun postExportEvent(bookUrl: String)

    /** 设置导出进度 (对应 exportProgress[bookUrl] = progress)。 */
    fun setExportProgress(bookUrl: String, progress: Int)
}
