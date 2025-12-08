#pragma once

#include <concepts>



template<typename Tag, std::integral T>
class Int16StrongType {
    static_assert(sizeof(T) == 2);
    T value;
    public:
    explicit Int16StrongType(T v) : value(v) {}
    Int16StrongType operator+(Int16StrongType other) const { return value + other.value; }
    Int16StrongType operator-(Int16StrongType other) const { return value - other.value; }
    Int16StrongType operator*(Int16StrongType other) const { return value * other.value; }
    Int16StrongType operator/(Int16StrongType other) const { return value / other.value; }

    bool operator==(Int16StrongType other) const { return value == other.value; }
    bool operator!=(Int16StrongType other) const { return value != other.value; }
    bool operator< (Int16StrongType other) const { return value <  other.value; }
    bool operator<=(Int16StrongType other) const { return value <= other.value; }
    bool operator> (Int16StrongType other) const { return value >  other.value; }
    bool operator>=(Int16StrongType other) const { return value >= other.value; }

    bool operator==(T other) const { return value == other.value; }
    bool operator!=(T other) const { return value != other.value; }
    bool operator< (T other) const { return value <  other.value; }
    bool operator<=(T other) const { return value <= other.value; }
    bool operator> (T other) const { return value >  other.value; }
    bool operator>=(T other) const { return value >= other.value; }

    // Can add to char ptr
    char* operator+(char* other) const { return value + other; }
    friend constexpr char* operator+(char* ptr, Int16StrongType offset) { return ptr + offset.value; }
    friend constexpr char* operator+(Int16StrongType offset, char* ptr) { return ptr + offset.value; }
};

namespace lib_internal {
    template<typename Tag, std::integral T>
    class Int16StrongType_Friend {
        using friend_t = Int16StrongType<Tag, T>;
        friend friend_t;
        friend_t get(friend_t a) { return a.value; }
    };
}

template<typename Tag, std::integral T>
char* operator+(Int16StrongType<Tag, T> a, char* other) { return lib_internal::Int16StrongType_Friend{a}.get() + other; }

namespace lib_internal{
    struct OffsetTag {};
}
using offset_t = Int16StrongType<lib_internal::OffsetTag, unsigned short>;