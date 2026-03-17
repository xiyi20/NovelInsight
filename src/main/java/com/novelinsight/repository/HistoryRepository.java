package com.novelinsight.repository;

import com.novelinsight.model.History;
import com.novelinsight.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface HistoryRepository extends JpaRepository<History, Long> {

    List<History> findByUserOrderByCountDesc(User user);

    Optional<History> findByUserAndNovelId(User user, String novelId);

    void deleteByUserAndNovelId(User user, String novelId);

    long countByUser(User user);
}