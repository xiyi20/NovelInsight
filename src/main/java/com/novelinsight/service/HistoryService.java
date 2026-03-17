package com.novelinsight.service;

import com.novelinsight.model.History;
import com.novelinsight.model.User;
import com.novelinsight.repository.HistoryRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * 历史记录服务，对应getChartData.py中的相关函数
 */
@Service
public class HistoryService {

    private final HistoryRepository historyRepository;
    private final HiveService hiveService;

    public HistoryService(HistoryRepository historyRepository, HiveService hiveService) {
        this.historyRepository = historyRepository;
        this.hiveService = hiveService;
    }

    /**
     * 添加历史记录（对应addHistoryData函数）
     */
    @Transactional
    public void addHistory(User user, String novelId) {
        Optional<History> existingHistory = historyRepository.findByUserAndNovelId(user, novelId);
        if (existingHistory.isPresent()) {
            History history = existingHistory.get();
            history.setCount(history.getCount() + 1);
            historyRepository.save(history);
        } else {
            History newHistory = new History(user, novelId);
            historyRepository.save(newHistory);
        }
    }

    /**
     * 删除历史记录（对应delHistory操作）
     */
    @Transactional
    public void deleteHistory(User user, String novelId) {
        historyRepository.deleteByUserAndNovelId(user, novelId);
    }

    /**
     * 获取用户历史记录（对应getUserHistory函数）
     */
    public List<Object[]> getUserHistory(User user) {
        // 获取用户的历史记录列表，按点击次数降序排序
        List<History> historyList = historyRepository.findByUserOrderByCountDesc(user);

        // 从Hive中获取小说详情
        List<Object[]> novelList = new java.util.ArrayList<>();
        for (History history : historyList) {
            String novelId = history.getNovelId();
            try {
                // 查询小说详情
                List<Object[]> novelData = hiveService.query(
                    "SELECT * FROM novelData WHERE id = ?",
                    Integer.parseInt(novelId)
                );
                if (!novelData.isEmpty()) {
                    novelList.add(novelData.get(0));
                }
            } catch (NumberFormatException e) {
                // 如果novelId不是数字，跳过
                continue;
            }
        }

        return novelList;
    }

    /**
     * 获取用户的历史记录数量
     */
    public long getHistoryCount(User user) {
        return historyRepository.countByUser(user);
    }
}