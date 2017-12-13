package com.wyu.ae.dao;

import com.wyu.ae.model.PlatformDimension;
import org.springframework.stereotype.Repository;

public interface PlatformDimensionDao {
    PlatformDimension getPlatformDimension(PlatformDimension platform);

    PlatformDimension getPlatformDimension(String platform);

    PlatformDimension getPlatformDimension(int id);
}