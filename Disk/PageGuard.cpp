

#include "PageGuard.h"



WritePageGuard& WritePageGuard::operator=(WritePageGuard&& other) noexcept {
    if (this != &other) {
        if (valid) {
            release();
        }

        release_func = std::move(other.release_func);
        page = other.page;
        valid = other.valid; // incase false
        other.valid = false;
    }
    return *this;
}
WritePageGuard::~WritePageGuard() noexcept {
    if (valid) {
        valid = false;
        release_func(page);
    }
}
void WritePageGuard::release() noexcept {
    if (valid) {
        valid = false;
        release_func(page);
    }
}


ReadPageGuard& ReadPageGuard::operator=(ReadPageGuard&& other) noexcept {
    if (this != &other) {
        if (valid) {
            release();
        }

        release_func = std::move(other.release_func);
        page = other.page;
        valid = other.valid; // incase false
        other.valid = false;
    }
    return *this;
}
ReadPageGuard::~ReadPageGuard() noexcept {
    if (valid) {
        valid = false;
        release_func(page);
    }
}
void ReadPageGuard::release() noexcept {
    if (valid) {
        valid = false;
        release_func(page);
    }
}

